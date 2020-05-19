use std::borrow::Cow;
use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::ops::Range;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::fmt;

use compact_arena::{SmallArena, Idx32, mk_arena};
use log::debug;
use meilisearch_types::DocIndex;
use sdset::{Set, SetBuf, exponential_search, SetOperation, Counter, duo::OpBuilder};
use slice_group_by::{GroupBy, GroupByMut};

use crate::error::Error;
use crate::criterion::{Context, ContextMut};
use crate::distinct_map::{BufferedDistinctMap, DistinctMap};
use crate::raw_document::RawDocument;
use crate::database::MainT;
use crate::{Document, DocumentId, MResult};
use crate::query_tree::{create_query_tree, traverse_query_tree};
use crate::query_tree::{Operation, QueryResult, QueryKind, QueryId, PostingsKey};
use crate::query_tree::Context as QTContext;
use crate::query_builder::QueryBuilder;

#[derive(Debug, Default)]
pub struct SortResult {
    pub documents: Vec<Document>,
    pub nb_hits: usize,
    pub exhaustive_nb_hit: bool,
    pub facets: Option<HashMap<String, HashMap<String, usize>>>,
    pub exhaustive_facet_count: Option<bool>,
}

pub fn bucket_sort(
    reader: &heed::RoTxn<MainT>,
    query: &str,
    range: Range<usize>,
    mut query_context: QueryBuilder,
) -> MResult<SortResult>
{
    // We delegate the filter work to the distinct query builder,
    // specifying a distinct rule that has no effect.
    if query_context.filter.is_some() {
        query_context.distinct = Some((Box::new(|_| None ), 1));
        return bucket_sort_with_distinct(
            reader,
            query,
            range,
            query_context,
        );
    }

    let mut result = SortResult::default();

    let words_set = match unsafe { query_context.index.main.static_words_fst(reader)? } {
        Some(words) => words,
        None => return Ok(SortResult::default()),
    };

    let stop_words = query_context.index.main.stop_words_fst(reader)?.unwrap_or_default();

    let context = QTContext {
        words_set,
        stop_words,
        synonyms: query_context.index.synonyms,
        postings_lists: query_context.index.postings_lists,
        prefix_postings_lists: query_context.index.prefix_postings_lists_cache,
    };

    let (operation, mapping) = create_query_tree(reader, &context, query)?;
    debug!("operation:\n{:?}", operation);
    debug!("mapping:\n{:?}", mapping);

    fn recurs_operation<'o>(map: &mut HashMap<QueryId, &'o QueryKind>, operation: &'o Operation) {
        match operation {
            Operation::And(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
            Operation::Or(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
            Operation::Query(query) => { map.insert(query.id, &query.kind); },
        }
    }

    let mut queries_kinds = HashMap::new();
    recurs_operation(&mut queries_kinds, &operation);

    let QueryResult { mut docids, queries } = traverse_query_tree(reader, &context, &operation)?;
    debug!("found {} documents", docids.len());
    debug!("number of postings {:?}", queries.len());

    if let Some(ref facets_docids) = query_context.facet_filter {
        let intersection = sdset::duo::OpBuilder::new(docids.as_ref(), facets_docids.as_set())
            .intersection()
            .into_set_buf();
        docids = Cow::Owned(intersection);
    }

    if let Some(f) = query_context.facets.take() {
        // hardcoded value, until approximation optimization
        result.exhaustive_facet_count = Some(true);
        result.facets = Some(facet_count(f, &docids));
    }

    let before = Instant::now();
    mk_arena!(arena);
    let mut bare_matches = cleanup_bare_matches(&mut arena, &docids, queries);
    debug!("matches cleaned in {:.02?}", before.elapsed());

    let before_bucket_sort = Instant::now();

    let before_raw_documents_building = Instant::now();
    let mut raw_documents = Vec::new();
    for bare_matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
        let raw_document = RawDocument::new(bare_matches, &mut arena, query_context.searchable_attrs.as_ref());
        raw_documents.push(raw_document);
    }
    debug!("creating {} candidates documents took {:.02?}",
        raw_documents.len(),
        before_raw_documents_building.elapsed(),
    );

    let before_criterion_loop = Instant::now();
    let proximity_count = AtomicUsize::new(0);

    let mut groups = vec![raw_documents.as_mut_slice()];

    'criteria: for criterion in query_context.criteria.as_ref() {
        let tmp_groups = mem::replace(&mut groups, Vec::new());
        let mut documents_seen = 0;

        for mut group in tmp_groups {
            let before_criterion_preparation = Instant::now();

            let ctx = ContextMut {
                reader,
                postings_lists: &mut arena,
                query_mapping: &mapping,
                documents_fields_counts_store: query_context.index.documents_fields_counts,
            };

            criterion.prepare(ctx, &mut group)?;
            debug!("{:?} preparation took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

            let ctx = Context {
                postings_lists: &arena,
                query_mapping: &mapping,
            };

            let before_criterion_sort = Instant::now();
            group.sort_unstable_by(|a, b| criterion.evaluate(&ctx, a, b));
            debug!("{:?} evaluation took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

            for group in group.binary_group_by_mut(|a, b| criterion.eq(&ctx, a, b)) {
                debug!("{:?} produced a group of size {}", criterion.name(), group.len());

                documents_seen += group.len();
                groups.push(group);

                // we have sort enough documents if the last document sorted is after
                // the end of the requested range, we can continue to the next criterion
                if documents_seen >= range.end {
                    continue 'criteria;
                }
            }
        }
    }

    debug!("criterion loop took {:.02?}", before_criterion_loop.elapsed());
    debug!("proximity evaluation called {} times", proximity_count.load(Ordering::Relaxed));

    let schema = query_context.index.main.schema(reader)?.ok_or(Error::SchemaMissing)?;
    let iter = raw_documents.into_iter().skip(range.start).take(range.len());
    let iter = iter.map(|rd| Document::from_raw(rd, &queries_kinds, &arena, query_context.searchable_attrs.as_ref(), &schema));
    let documents = iter.collect();

    debug!("bucket sort took {:.02?}", before_bucket_sort.elapsed());

    result.documents = documents;
    result.nb_hits = docids.len();

    Ok(result)
}

pub fn bucket_sort_with_distinct(
    reader: &heed::RoTxn<MainT>,
    query: &str,
    range: Range<usize>,
    query_context: QueryBuilder,
) -> MResult<SortResult>
{
    let mut result = SortResult::default();

    let (distinct, distinct_size) = query_context.distinct.expect("Bucket_sort_with_distinct need distinct");

    let words_set = match unsafe { query_context.index.main.static_words_fst(reader)? } {
        Some(words) => words,
        None => return Ok(SortResult::default()),
    };

    let stop_words = query_context.index.main.stop_words_fst(reader)?.unwrap_or_default();

    let context = QTContext {
        words_set,
        stop_words,
        synonyms: query_context.index.synonyms,
        postings_lists: query_context.index.postings_lists,
        prefix_postings_lists: query_context.index.prefix_postings_lists_cache,
    };

    let (operation, mapping) = create_query_tree(reader, &context, query)?;
    debug!("operation:\n{:?}", operation);
    debug!("mapping:\n{:?}", mapping);

    fn recurs_operation<'o>(map: &mut HashMap<QueryId, &'o QueryKind>, operation: &'o Operation) {
        match operation {
            Operation::And(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
            Operation::Or(ops) => ops.iter().for_each(|op| recurs_operation(map, op)),
            Operation::Query(query) => { map.insert(query.id, &query.kind); },
        }
    }

    let mut queries_kinds = HashMap::new();
    recurs_operation(&mut queries_kinds, &operation);

    let QueryResult { mut docids, queries } = traverse_query_tree(reader, &context, &operation)?;
    debug!("found {} documents", docids.len());
    debug!("number of postings {:?}", queries.len());

    if let Some(facets_docids) = query_context.facet_filter {
        let intersection = OpBuilder::new(docids.as_ref(), facets_docids.as_set())
            .intersection()
            .into_set_buf();
        docids = Cow::Owned(intersection);
    }

    if let Some(f) = query_context.facets {
        // hardcoded value, until approximation optimization
        result.exhaustive_facet_count = Some(true);
        result.facets = Some(facet_count(f, &docids));
    }

    let before = Instant::now();
    mk_arena!(arena);
    let mut bare_matches = cleanup_bare_matches(&mut arena, &docids, queries);
    debug!("matches cleaned in {:.02?}", before.elapsed());

    let before_raw_documents_building = Instant::now();
    let mut raw_documents = Vec::new();
    for bare_matches in bare_matches.linear_group_by_key_mut(|sm| sm.document_id) {
        let raw_document = RawDocument::new(bare_matches, &mut arena, query_context.searchable_attrs.as_ref());
        raw_documents.push(raw_document);
    }
    debug!("creating {} candidates documents took {:.02?}",
        raw_documents.len(),
        before_raw_documents_building.elapsed(),
    );

    let mut groups = vec![raw_documents.as_mut_slice()];
    let mut key_cache = HashMap::new();

    let mut filter_map = HashMap::new();
    // these two variables informs on the current distinct map and
    // on the raw offset of the start of the group where the
    // range.start bound is located according to the distinct function
    let mut distinct_map = DistinctMap::new(distinct_size);
    let mut distinct_raw_offset = 0;

    'criteria: for criterion in query_context.criteria.as_ref() {
        let tmp_groups = mem::replace(&mut groups, Vec::new());
        let mut buf_distinct = BufferedDistinctMap::new(&mut distinct_map);
        let mut documents_seen = 0;

        for mut group in tmp_groups {
            // if this group does not overlap with the requested range,
            // push it without sorting and splitting it
            if documents_seen + group.len() < distinct_raw_offset {
                documents_seen += group.len();
                groups.push(group);
                continue;
            }

            let ctx = ContextMut {
                reader,
                postings_lists: &mut arena,
                query_mapping: &mapping,
                documents_fields_counts_store: query_context.index.documents_fields_counts,
            };

            let before_criterion_preparation = Instant::now();
            criterion.prepare(ctx, &mut group)?;
            debug!("{:?} preparation took {:.02?}", criterion.name(), before_criterion_preparation.elapsed());

            let ctx = Context {
                postings_lists: &arena,
                query_mapping: &mapping,
            };

            let before_criterion_sort = Instant::now();
            group.sort_unstable_by(|a, b| criterion.evaluate(&ctx, a, b));
            debug!("{:?} evaluation took {:.02?}", criterion.name(), before_criterion_sort.elapsed());

            for group in group.binary_group_by_mut(|a, b| criterion.eq(&ctx, a, b)) {
                // we must compute the real distinguished len of this sub-group
                for document in group.iter() {
                    let filter_accepted = match &query_context.filter {
                        Some(filter) => {
                            let entry = filter_map.entry(document.id);
                            *entry.or_insert_with(|| (filter)(document.id))
                        }
                        None => true,
                    };

                    if filter_accepted {
                        let entry = key_cache.entry(document.id);
                        let key = entry.or_insert_with(|| (distinct)(document.id).map(Rc::new));

                        match key.clone() {
                            Some(key) => buf_distinct.register(key),
                            None => buf_distinct.register_without_key(),
                        };
                    }

                    // the requested range end is reached: stop computing distinct
                    if buf_distinct.len() >= range.end {
                        break;
                    }
                }

                documents_seen += group.len();
                groups.push(group);

                // if this sub-group does not overlap with the requested range
                // we must update the distinct map and its start index
                if buf_distinct.len() < range.start {
                    buf_distinct.transfert_to_internal();
                    distinct_raw_offset = documents_seen;
                }

                // we have sort enough documents if the last document sorted is after
                // the end of the requested range, we can continue to the next criterion
                if buf_distinct.len() >= range.end {
                    continue 'criteria;
                }
            }
        }
    }

    // once we classified the documents related to the current
    // automatons we save that as the next valid result
    let mut seen = BufferedDistinctMap::new(&mut distinct_map);
    let schema = query_context.index.main.schema(reader)?.ok_or(Error::SchemaMissing)?;

    let mut documents = Vec::with_capacity(range.len());
    for raw_document in raw_documents.into_iter().skip(distinct_raw_offset) {
        let filter_accepted = match &query_context.filter {
            Some(_) => filter_map.remove(&raw_document.id).unwrap(),
            None => true,
        };

        if filter_accepted {
            let key = key_cache.remove(&raw_document.id).unwrap();
            let distinct_accepted = match key {
                Some(key) => seen.register(key),
                None => seen.register_without_key(),
            };

            if distinct_accepted && seen.len() > range.start {
                documents.push(Document::from_raw(raw_document, &queries_kinds, &arena, query_context.searchable_attrs.as_ref(), &schema));
                if documents.len() == range.len() {
                    break;
                }
            }
        }
    }
    result.documents = documents;
    result.nb_hits = docids.len();

    Ok(result)
}

fn cleanup_bare_matches<'tag, 'txn>(
    arena: &mut SmallArena<'tag, PostingsListView<'txn>>,
    docids: &Set<DocumentId>,
    queries: HashMap<PostingsKey, Cow<'txn, Set<DocIndex>>>,
) -> Vec<BareMatch<'tag>>
{
    let docidslen = docids.len() as f32;
    let mut bare_matches = Vec::new();

    for (PostingsKey { query, input, distance, is_exact }, matches) in queries {
        let postings_list_view = PostingsListView::original(Rc::from(input), Rc::new(matches));
        let pllen = postings_list_view.len() as f32;

        if docidslen / pllen >= 0.8 {
            let mut offset = 0;
            for matches in postings_list_view.linear_group_by_key(|m| m.document_id) {
                let document_id = matches[0].document_id;
                if docids.contains(&document_id) {
                    let range = postings_list_view.range(offset, matches.len());
                    let posting_list_index = arena.add(range);

                    let bare_match = BareMatch {
                        document_id,
                        query_index: query.id,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    bare_matches.push(bare_match);
                }

                offset += matches.len();
            }

        } else {
            let mut offset = 0;
            for id in docids.as_slice() {
                let di = DocIndex { document_id: *id, ..DocIndex::default() };
                let pos = exponential_search(&postings_list_view[offset..], &di).unwrap_or_else(|x| x);

                offset += pos;

                let group = postings_list_view[offset..]
                    .linear_group_by_key(|m| m.document_id)
                    .next()
                    .filter(|matches| matches[0].document_id == *id);

                if let Some(matches) = group {
                    let range = postings_list_view.range(offset, matches.len());
                    let posting_list_index = arena.add(range);

                    let bare_match = BareMatch {
                        document_id: *id,
                        query_index: query.id,
                        distance,
                        is_exact,
                        postings_list: posting_list_index,
                    };

                    bare_matches.push(bare_match);
                }
            }
        }
    }

    let before_raw_documents_presort = Instant::now();
    bare_matches.sort_unstable_by_key(|sm| sm.document_id);
    debug!("sort by documents ids took {:.02?}", before_raw_documents_presort.elapsed());

    bare_matches
}

pub struct BareMatch<'tag> {
    pub document_id: DocumentId,
    pub query_index: usize,
    pub distance: u8,
    pub is_exact: bool,
    pub postings_list: Idx32<'tag>,
}

impl fmt::Debug for BareMatch<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BareMatch")
            .field("document_id", &self.document_id)
            .field("query_index", &self.query_index)
            .field("distance", &self.distance)
            .field("is_exact", &self.is_exact)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimpleMatch {
    pub query_index: usize,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
}

#[derive(Clone)]
pub enum PostingsListView<'txn> {
    Original {
        input: Rc<[u8]>,
        postings_list: Rc<Cow<'txn, Set<DocIndex>>>,
        offset: usize,
        len: usize,
    },
    Rewritten {
        input: Rc<[u8]>,
        postings_list: SetBuf<DocIndex>,
    },
}

impl fmt::Debug for PostingsListView<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostingsListView")
            .field("input", &std::str::from_utf8(&self.input()).unwrap())
            .field("postings_list", &self.as_ref())
            .finish()
    }
}

impl<'txn> PostingsListView<'txn> {
    pub fn original(input: Rc<[u8]>, postings_list: Rc<Cow<'txn, Set<DocIndex>>>) -> PostingsListView<'txn> {
        let len = postings_list.len();
        PostingsListView::Original { input, postings_list, offset: 0, len }
    }

    pub fn rewritten(input: Rc<[u8]>, postings_list: SetBuf<DocIndex>) -> PostingsListView<'txn> {
        PostingsListView::Rewritten { input, postings_list }
    }

    pub fn rewrite_with(&mut self, postings_list: SetBuf<DocIndex>) {
        let input = match self {
            PostingsListView::Original { input, .. } => input.clone(),
            PostingsListView::Rewritten { input, .. } => input.clone(),
        };
        *self = PostingsListView::rewritten(input, postings_list);
    }

    pub fn len(&self) -> usize {
        match self {
            PostingsListView::Original { len, .. } => *len,
            PostingsListView::Rewritten { postings_list, .. } => postings_list.len(),
        }
    }

    pub fn input(&self) -> &[u8] {
        match self {
            PostingsListView::Original { ref input, .. } => input,
            PostingsListView::Rewritten { ref input, .. } => input,
        }
    }

    pub fn range(&self, range_offset: usize, range_len: usize) -> PostingsListView<'txn> {
        match self {
            PostingsListView::Original { input, postings_list, offset, len } => {
                assert!(range_offset + range_len <= *len);
                PostingsListView::Original {
                    input: input.clone(),
                    postings_list: postings_list.clone(),
                    offset: offset + range_offset,
                    len: range_len,
                }
            },
            PostingsListView::Rewritten { .. } => {
                panic!("Cannot create a range on a rewritten postings list view");
            }
        }
    }
}

impl AsRef<Set<DocIndex>> for PostingsListView<'_> {
    fn as_ref(&self) -> &Set<DocIndex> {
        self
    }
}

impl Deref for PostingsListView<'_> {
    type Target = Set<DocIndex>;

    fn deref(&self) -> &Set<DocIndex> {
        match *self {
            PostingsListView::Original { ref postings_list, offset, len, .. } => {
                Set::new_unchecked(&postings_list[offset..offset + len])
            },
            PostingsListView::Rewritten { ref postings_list, .. } => postings_list,
        }
    }
}

/// For each entry in facet_docids, calculates the number of documents in the intersection with candidate_docids.
fn facet_count(
    facet_docids: HashMap<String, HashMap<String, Cow<Set<DocumentId>>>>,
    candidate_docids: &Set<DocumentId>,
) -> HashMap<String, HashMap<String, usize>> {
    let mut facets_counts = HashMap::with_capacity(facet_docids.len());
    for (key, doc_map) in facet_docids {
        let mut count_map = HashMap::with_capacity(doc_map.len());
        for (value, docids) in doc_map {
            let mut counter = Counter::new();
            let op = OpBuilder::new(docids.as_ref(), candidate_docids).intersection();
            SetOperation::<DocumentId>::extend_collection(op, &mut counter);
            count_map.insert(value, counter.0);
        }
        facets_counts.insert(key, count_map);
    }
    facets_counts
}
