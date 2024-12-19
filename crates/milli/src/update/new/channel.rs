use std::cell::RefCell;
use std::io::{self, BufWriter};
use std::iter::Cycle;
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroU16;
use std::ops::Range;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use bbqueue::framed::{FrameGrantR, FrameProducer};
use bbqueue::BBBuffer;
use bytemuck::{checked, CheckedBitPattern, NoUninit};
use flume::{RecvTimeoutError, SendError};
use heed::types::Bytes;
use heed::{BytesDecode, MdbError};
use memmap2::{Mmap, MmapMut};
use roaring::RoaringBitmap;

use super::extract::FacetKind;
use super::ref_cell_ext::RefCellExt;
use super::thread_local::{FullySend, ThreadLocal};
use super::StdResult;
use crate::heed_codec::facet::{FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec};
use crate::index::db_name;
use crate::index::main_key::{GEO_FACETED_DOCUMENTS_IDS_KEY, GEO_RTREE_KEY};
use crate::update::new::KvReaderFieldId;
use crate::vector::Embedding;
use crate::{CboRoaringBitmapCodec, DocumentId, Error, Index, InternalError};

/// Creates a tuple of senders/receiver to be used by
/// the extractors and the writer loop.
///
/// The `total_bbbuffer_capacity` represents the number of bytes
/// allocated to all BBQueue buffers. It will be split by the
/// number of threads.
///
/// The `channel_capacity` parameter defines the number of
/// too-large-to-fit-in-BBQueue entries that can be sent through
/// a flume channel. This parameter must stay low to make
/// sure we do not use too much memory.
///
/// Note that the channel is also used to wake-up the receiver
/// when new stuff is available in any BBQueue buffer but we send
/// a message in this queue only if it is empty to avoid filling
/// the channel *and* the BBQueue.
pub fn extractor_writer_bbqueue(
    bbbuffers: &mut Vec<BBBuffer>,
    total_bbbuffer_capacity: usize,
    channel_capacity: usize,
) -> (ExtractorBbqueueSender, WriterBbqueueReceiver) {
    let current_num_threads = rayon::current_num_threads();
    let bbbuffer_capacity = total_bbbuffer_capacity.checked_div(current_num_threads).unwrap();
    bbbuffers.resize_with(current_num_threads, || BBBuffer::new(bbbuffer_capacity));

    let capacity = bbbuffers.first().unwrap().capacity();
    // Read the field description to understand this
    let capacity = capacity.checked_sub(9).unwrap();

    let producers = ThreadLocal::with_capacity(bbbuffers.len());
    let consumers = rayon::broadcast(|bi| {
        let bbqueue = &bbbuffers[bi.index()];
        let (producer, consumer) = bbqueue.try_split_framed().unwrap();
        producers.get_or(|| FullySend(RefCell::new(producer)));
        consumer
    });

    let sent_messages_attempts = Arc::new(AtomicUsize::new(0));
    let blocking_sent_messages_attempts = Arc::new(AtomicUsize::new(0));

    let (sender, receiver) = flume::bounded(channel_capacity);
    let sender = ExtractorBbqueueSender {
        sender,
        producers,
        capacity,
        sent_messages_attempts: sent_messages_attempts.clone(),
        blocking_sent_messages_attempts: blocking_sent_messages_attempts.clone(),
    };
    let receiver = WriterBbqueueReceiver {
        receiver,
        look_at_consumer: (0..consumers.len()).cycle(),
        consumers,
        sent_messages_attempts,
        blocking_sent_messages_attempts,
    };
    (sender, receiver)
}

pub struct ExtractorBbqueueSender<'a> {
    /// This channel is used to wake-up the receiver and
    /// send large entries that cannot fit in the BBQueue.
    sender: flume::Sender<ReceiverAction>,
    /// A memory buffer, one by thread, is used to serialize
    /// the entries directly in this shared, lock-free space.
    producers: ThreadLocal<FullySend<RefCell<FrameProducer<'a>>>>,
    /// The capacity of this frame producer, will never be able to store more than that.
    ///
    /// Note that the FrameProducer requires up to 9 bytes to encode the length,
    /// the capacity has been shrunk accordingly.
    ///
    /// <https://docs.rs/bbqueue/latest/bbqueue/framed/index.html#frame-header>
    capacity: usize,
    /// The total number of attempts to send messages
    /// over the bbqueue channel.
    sent_messages_attempts: Arc<AtomicUsize>,
    /// The number of times an attempt to send a
    /// messages failed and we had to pause for a bit.
    blocking_sent_messages_attempts: Arc<AtomicUsize>,
}

pub struct WriterBbqueueReceiver<'a> {
    /// Used to wake up when new entries are available either in
    /// any BBQueue buffer or directly sent throught this channel
    /// (still written to disk).
    receiver: flume::Receiver<ReceiverAction>,
    /// Indicates the consumer to observe. This cycling range
    /// ensures fair distribution of work among consumers.
    look_at_consumer: Cycle<Range<usize>>,
    /// The BBQueue frames to read when waking-up.
    consumers: Vec<bbqueue::framed::FrameConsumer<'a>>,
    /// The total number of attempts to send messages
    /// over the bbqueue channel.
    sent_messages_attempts: Arc<AtomicUsize>,
    /// The number of times an attempt to send a
    /// messages failed and we had to pause for a bit.
    blocking_sent_messages_attempts: Arc<AtomicUsize>,
}

/// The action to perform on the receiver/writer side.
#[derive(Debug)]
pub enum ReceiverAction {
    /// Wake up, you have frames to read for the BBQueue buffers.
    WakeUp,
    LargeEntry(LargeEntry),
    LargeVectors(LargeVectors),
}

/// An entry that cannot fit in the BBQueue buffers has been
/// written to disk, memory-mapped and must be written in the
/// database.
#[derive(Debug)]
pub struct LargeEntry {
    /// The database where the entry must be written.
    pub database: Database,
    /// The key of the entry that must be written in the database.
    pub key: Box<[u8]>,
    /// The large value that must be written.
    ///
    /// Note: We can probably use a `File` here and
    /// use `Database::put_reserved` instead of memory-mapping.
    pub value: Mmap,
}

/// When embeddings are larger than the available
/// BBQueue space it arrives here.
#[derive(Debug)]
pub struct LargeVectors {
    /// The document id associated to the large embedding.
    pub docid: DocumentId,
    /// The embedder id in which to insert the large embedding.
    pub embedder_id: u8,
    /// The large embedding that must be written.
    pub embeddings: Mmap,
}

impl LargeVectors {
    pub fn read_embeddings(&self, dimensions: usize) -> impl Iterator<Item = &[f32]> {
        self.embeddings.chunks_exact(dimensions).map(bytemuck::cast_slice)
    }
}

impl<'a> WriterBbqueueReceiver<'a> {
    /// Tries to receive an action to do until the timeout occurs
    /// and if it does, consider it as a spurious wake up.
    pub fn recv_action(&mut self) -> Option<ReceiverAction> {
        match self.receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(action) => Some(action),
            Err(RecvTimeoutError::Timeout) => Some(ReceiverAction::WakeUp),
            Err(RecvTimeoutError::Disconnected) => None,
        }
    }

    /// Reads all the BBQueue buffers and selects the first available frame.
    pub fn recv_frame(&mut self) -> Option<FrameWithHeader<'a>> {
        for index in self.look_at_consumer.by_ref().take(self.consumers.len()) {
            if let Some(frame) = self.consumers[index].read() {
                return Some(FrameWithHeader::from(frame));
            }
        }
        None
    }

    /// Returns the total count of attempts to send messages through the BBQueue channel.
    pub fn sent_messages_attempts(&self) -> usize {
        self.sent_messages_attempts.load(atomic::Ordering::Relaxed)
    }

    /// Returns the count of attempts to send messages that had to be paused due to BBQueue being full.
    pub fn blocking_sent_messages_attempts(&self) -> usize {
        self.blocking_sent_messages_attempts.load(atomic::Ordering::Relaxed)
    }
}

pub struct FrameWithHeader<'a> {
    header: EntryHeader,
    frame: FrameGrantR<'a>,
}

impl FrameWithHeader<'_> {
    pub fn header(&self) -> EntryHeader {
        self.header
    }

    pub fn frame(&self) -> &FrameGrantR<'_> {
        &self.frame
    }
}

impl<'a> From<FrameGrantR<'a>> for FrameWithHeader<'a> {
    fn from(mut frame: FrameGrantR<'a>) -> Self {
        frame.auto_release(true);
        FrameWithHeader { header: EntryHeader::from_slice(&frame[..]), frame }
    }
}

/// A header that is written at the beginning of a bbqueue frame.
///
/// Note that the different variants cannot be changed without taking
/// care of their size in the implementation, like, everywhere.
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum EntryHeader {
    DbOperation(DbOperation),
    ArroyDeleteVector(ArroyDeleteVector),
    ArroySetVectors(ArroySetVectors),
}

impl EntryHeader {
    const fn variant_size() -> usize {
        mem::size_of::<u8>()
    }

    const fn variant_id(&self) -> u8 {
        match self {
            EntryHeader::DbOperation(_) => 0,
            EntryHeader::ArroyDeleteVector(_) => 1,
            EntryHeader::ArroySetVectors(_) => 2,
        }
    }

    const fn total_key_value_size(key_length: NonZeroU16, value_length: usize) -> usize {
        Self::variant_size()
            + mem::size_of::<DbOperation>()
            + key_length.get() as usize
            + value_length
    }

    const fn total_key_size(key_length: NonZeroU16) -> usize {
        Self::total_key_value_size(key_length, 0)
    }

    const fn total_delete_vector_size() -> usize {
        Self::variant_size() + mem::size_of::<ArroyDeleteVector>()
    }

    /// The `dimensions` corresponds to the number of `f32` in the embedding.
    fn total_set_vectors_size(count: usize, dimensions: usize) -> usize {
        let embedding_size = dimensions * mem::size_of::<f32>();
        Self::variant_size() + mem::size_of::<ArroySetVectors>() + embedding_size * count
    }

    fn header_size(&self) -> usize {
        let payload_size = match self {
            EntryHeader::DbOperation(op) => mem::size_of_val(op),
            EntryHeader::ArroyDeleteVector(adv) => mem::size_of_val(adv),
            EntryHeader::ArroySetVectors(asvs) => mem::size_of_val(asvs),
        };
        Self::variant_size() + payload_size
    }

    fn from_slice(slice: &[u8]) -> EntryHeader {
        let (variant_id, remaining) = slice.split_first().unwrap();
        match variant_id {
            0 => {
                let header_bytes = &remaining[..mem::size_of::<DbOperation>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::DbOperation(header)
            }
            1 => {
                let header_bytes = &remaining[..mem::size_of::<ArroyDeleteVector>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::ArroyDeleteVector(header)
            }
            2 => {
                let header_bytes = &remaining[..mem::size_of::<ArroySetVectors>()];
                let header = checked::pod_read_unaligned(header_bytes);
                EntryHeader::ArroySetVectors(header)
            }
            id => panic!("invalid variant id: {id}"),
        }
    }

    fn serialize_into(&self, header_bytes: &mut [u8]) {
        let (first, remaining) = header_bytes.split_first_mut().unwrap();
        let payload_bytes = match self {
            EntryHeader::DbOperation(op) => bytemuck::bytes_of(op),
            EntryHeader::ArroyDeleteVector(adv) => bytemuck::bytes_of(adv),
            EntryHeader::ArroySetVectors(asvs) => bytemuck::bytes_of(asvs),
        };
        *first = self.variant_id();
        remaining.copy_from_slice(payload_bytes);
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
/// Wether a put of the key/value pair or a delete of the given key.
pub struct DbOperation {
    /// The database on which to perform the operation.
    pub database: Database,
    /// The key length in the buffer.
    ///
    /// If None it means that the buffer is dedicated
    /// to the key and it is therefore a deletion operation.
    pub key_length: Option<NonZeroU16>,
}

impl DbOperation {
    pub fn key_value<'a>(&self, frame: &'a FrameGrantR<'_>) -> (&'a [u8], Option<&'a [u8]>) {
        let skip = EntryHeader::variant_size() + mem::size_of::<Self>();
        match self.key_length {
            Some(key_length) => {
                let (key, value) = frame[skip..].split_at(key_length.get() as usize);
                (key, Some(value))
            }
            None => (&frame[skip..], None),
        }
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(transparent)]
pub struct ArroyDeleteVector {
    pub docid: DocumentId,
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(C)]
/// The embeddings are in the remaining space and represents
/// non-aligned [f32] each with dimensions f32s.
pub struct ArroySetVectors {
    pub docid: DocumentId,
    pub embedder_id: u8,
    _padding: [u8; 3],
}

impl ArroySetVectors {
    fn embeddings_bytes<'a>(frame: &'a FrameGrantR<'_>) -> &'a [u8] {
        let skip = EntryHeader::variant_size() + mem::size_of::<Self>();
        &frame[skip..]
    }

    /// Read all the embeddings and write them into an aligned `f32` Vec.
    pub fn read_all_embeddings_into_vec<'v>(
        &self,
        frame: &FrameGrantR<'_>,
        vec: &'v mut Vec<f32>,
    ) -> &'v [f32] {
        let embeddings_bytes = Self::embeddings_bytes(frame);
        let embeddings_count = embeddings_bytes.len() / mem::size_of::<f32>();
        vec.resize(embeddings_count, 0.0);
        bytemuck::cast_slice_mut(vec.as_mut()).copy_from_slice(embeddings_bytes);
        &vec[..]
    }
}

#[derive(Debug, Clone, Copy, NoUninit, CheckedBitPattern)]
#[repr(u16)]
pub enum Database {
    Main,
    Documents,
    ExternalDocumentsIds,
    ExactWordDocids,
    FidWordCountDocids,
    WordDocids,
    WordFidDocids,
    WordPairProximityDocids,
    WordPositionDocids,
    FacetIdIsNullDocids,
    FacetIdIsEmptyDocids,
    FacetIdExistsDocids,
    FacetIdF64Docids,
    FacetIdStringDocids,
    FieldIdDocidFacetStrings,
    FieldIdDocidFacetF64s,
}

impl Database {
    pub fn database(&self, index: &Index) -> heed::Database<Bytes, Bytes> {
        match self {
            Database::Main => index.main.remap_types(),
            Database::Documents => index.documents.remap_types(),
            Database::ExternalDocumentsIds => index.external_documents_ids.remap_types(),
            Database::ExactWordDocids => index.exact_word_docids.remap_types(),
            Database::WordDocids => index.word_docids.remap_types(),
            Database::WordFidDocids => index.word_fid_docids.remap_types(),
            Database::WordPositionDocids => index.word_position_docids.remap_types(),
            Database::FidWordCountDocids => index.field_id_word_count_docids.remap_types(),
            Database::WordPairProximityDocids => index.word_pair_proximity_docids.remap_types(),
            Database::FacetIdIsNullDocids => index.facet_id_is_null_docids.remap_types(),
            Database::FacetIdIsEmptyDocids => index.facet_id_is_empty_docids.remap_types(),
            Database::FacetIdExistsDocids => index.facet_id_exists_docids.remap_types(),
            Database::FacetIdF64Docids => index.facet_id_f64_docids.remap_types(),
            Database::FacetIdStringDocids => index.facet_id_string_docids.remap_types(),
            Database::FieldIdDocidFacetStrings => index.field_id_docid_facet_strings.remap_types(),
            Database::FieldIdDocidFacetF64s => index.field_id_docid_facet_f64s.remap_types(),
        }
    }

    pub fn database_name(&self) -> &'static str {
        match self {
            Database::Main => db_name::MAIN,
            Database::Documents => db_name::DOCUMENTS,
            Database::ExternalDocumentsIds => db_name::EXTERNAL_DOCUMENTS_IDS,
            Database::ExactWordDocids => db_name::EXACT_WORD_DOCIDS,
            Database::WordDocids => db_name::WORD_DOCIDS,
            Database::WordFidDocids => db_name::WORD_FIELD_ID_DOCIDS,
            Database::WordPositionDocids => db_name::WORD_POSITION_DOCIDS,
            Database::FidWordCountDocids => db_name::FIELD_ID_WORD_COUNT_DOCIDS,
            Database::WordPairProximityDocids => db_name::WORD_PAIR_PROXIMITY_DOCIDS,
            Database::FacetIdIsNullDocids => db_name::FACET_ID_IS_NULL_DOCIDS,
            Database::FacetIdIsEmptyDocids => db_name::FACET_ID_IS_EMPTY_DOCIDS,
            Database::FacetIdExistsDocids => db_name::FACET_ID_EXISTS_DOCIDS,
            Database::FacetIdF64Docids => db_name::FACET_ID_F64_DOCIDS,
            Database::FacetIdStringDocids => db_name::FACET_ID_STRING_DOCIDS,
            Database::FieldIdDocidFacetStrings => db_name::FIELD_ID_DOCID_FACET_STRINGS,
            Database::FieldIdDocidFacetF64s => db_name::FIELD_ID_DOCID_FACET_F64S,
        }
    }
}

impl From<FacetKind> for Database {
    fn from(value: FacetKind) -> Self {
        match value {
            FacetKind::Number => Database::FacetIdF64Docids,
            FacetKind::String => Database::FacetIdStringDocids,
            FacetKind::Null => Database::FacetIdIsNullDocids,
            FacetKind::Empty => Database::FacetIdIsEmptyDocids,
            FacetKind::Exists => Database::FacetIdExistsDocids,
        }
    }
}

impl<'b> ExtractorBbqueueSender<'b> {
    pub fn docids<'a, D: DatabaseType>(&'a self) -> WordDocidsSender<'a, 'b, D> {
        WordDocidsSender { sender: self, _marker: PhantomData }
    }

    pub fn facet_docids<'a>(&'a self) -> FacetDocidsSender<'a, 'b> {
        FacetDocidsSender { sender: self }
    }

    pub fn field_id_docid_facet_sender<'a>(&'a self) -> FieldIdDocidFacetSender<'a, 'b> {
        FieldIdDocidFacetSender(self)
    }

    pub fn documents<'a>(&'a self) -> DocumentsSender<'a, 'b> {
        DocumentsSender(self)
    }

    pub fn embeddings<'a>(&'a self) -> EmbeddingSender<'a, 'b> {
        EmbeddingSender(self)
    }

    pub fn geo<'a>(&'a self) -> GeoSender<'a, 'b> {
        GeoSender(self)
    }

    fn delete_vector(&self, docid: DocumentId) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let payload_header = EntryHeader::ArroyDeleteVector(ArroyDeleteVector { docid });
        let total_length = EntryHeader::total_delete_vector_size();
        if total_length > capacity {
            panic!("The entry is larger ({total_length} bytes) than the BBQueue capacity ({capacity} bytes)");
        }

        // Spin loop to have a frame the size we requested.
        reserve_and_write_grant(
            &mut producer,
            total_length,
            &self.sender,
            &self.sent_messages_attempts,
            &self.blocking_sent_messages_attempts,
            |grant| {
                payload_header.serialize_into(grant);
                Ok(())
            },
        )?;

        Ok(())
    }

    fn set_vectors(
        &self,
        docid: u32,
        embedder_id: u8,
        embeddings: &[Vec<f32>],
    ) -> crate::Result<()> {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        // If there are no vectors we specify the dimensions
        // to zero to allocate no extra space at all
        let dimensions = embeddings.first().map_or(0, |emb| emb.len());

        let arroy_set_vector = ArroySetVectors { docid, embedder_id, _padding: [0; 3] };
        let payload_header = EntryHeader::ArroySetVectors(arroy_set_vector);
        let total_length = EntryHeader::total_set_vectors_size(embeddings.len(), dimensions);
        if total_length > capacity {
            let mut value_file = tempfile::tempfile().map(BufWriter::new)?;
            for embedding in embeddings {
                let mut embedding_bytes = bytemuck::cast_slice(embedding);
                io::copy(&mut embedding_bytes, &mut value_file)?;
            }

            let value_file = value_file.into_inner().map_err(|ie| ie.into_error())?;
            let embeddings = unsafe { Mmap::map(&value_file)? };

            let large_vectors = LargeVectors { docid, embedder_id, embeddings };
            self.sender.send(ReceiverAction::LargeVectors(large_vectors)).unwrap();

            return Ok(());
        }

        // Spin loop to have a frame the size we requested.
        reserve_and_write_grant(
            &mut producer,
            total_length,
            &self.sender,
            &self.sent_messages_attempts,
            &self.blocking_sent_messages_attempts,
            |grant| {
                let header_size = payload_header.header_size();
                let (header_bytes, remaining) = grant.split_at_mut(header_size);
                payload_header.serialize_into(header_bytes);

                if dimensions != 0 {
                    let output_iter =
                        remaining.chunks_exact_mut(dimensions * mem::size_of::<f32>());
                    for (embedding, output) in embeddings.iter().zip(output_iter) {
                        output.copy_from_slice(bytemuck::cast_slice(embedding));
                    }
                }

                Ok(())
            },
        )?;

        Ok(())
    }

    fn write_key_value(&self, database: Database, key: &[u8], value: &[u8]) -> crate::Result<()> {
        let key_length = key.len().try_into().ok().and_then(NonZeroU16::new).ok_or_else(|| {
            InternalError::StorePut {
                database_name: database.database_name(),
                key: key.into(),
                value_length: value.len(),
                error: MdbError::BadValSize.into(),
            }
        })?;
        self.write_key_value_with(database, key_length, value.len(), |key_buffer, value_buffer| {
            key_buffer.copy_from_slice(key);
            value_buffer.copy_from_slice(value);
            Ok(())
        })
    }

    fn write_key_value_with<F>(
        &self,
        database: Database,
        key_length: NonZeroU16,
        value_length: usize,
        key_value_writer: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&mut [u8], &mut [u8]) -> crate::Result<()>,
    {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        let operation = DbOperation { database, key_length: Some(key_length) };
        let payload_header = EntryHeader::DbOperation(operation);
        let total_length = EntryHeader::total_key_value_size(key_length, value_length);
        if total_length > capacity {
            let mut key_buffer = vec![0; key_length.get() as usize].into_boxed_slice();
            let value_file = tempfile::tempfile()?;
            value_file.set_len(value_length.try_into().unwrap())?;
            let mut mmap_mut = unsafe { MmapMut::map_mut(&value_file)? };

            key_value_writer(&mut key_buffer, &mut mmap_mut)?;

            self.sender
                .send(ReceiverAction::LargeEntry(LargeEntry {
                    database,
                    key: key_buffer,
                    value: mmap_mut.make_read_only()?,
                }))
                .unwrap();

            return Ok(());
        }

        // Spin loop to have a frame the size we requested.
        reserve_and_write_grant(
            &mut producer,
            total_length,
            &self.sender,
            &self.sent_messages_attempts,
            &self.blocking_sent_messages_attempts,
            |grant| {
                let header_size = payload_header.header_size();
                let (header_bytes, remaining) = grant.split_at_mut(header_size);
                payload_header.serialize_into(header_bytes);
                let (key_buffer, value_buffer) = remaining.split_at_mut(key_length.get() as usize);
                key_value_writer(key_buffer, value_buffer)
            },
        )?;

        Ok(())
    }

    fn delete_entry(&self, database: Database, key: &[u8]) -> crate::Result<()> {
        let key_length = key.len().try_into().ok().and_then(NonZeroU16::new).ok_or_else(|| {
            InternalError::StoreDeletion {
                database_name: database.database_name(),
                key: key.into(),
                error: MdbError::BadValSize.into(),
            }
        })?;
        self.delete_entry_with(database, key_length, |buffer| {
            buffer.copy_from_slice(key);
            Ok(())
        })
    }

    fn delete_entry_with<F>(
        &self,
        database: Database,
        key_length: NonZeroU16,
        key_writer: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(&mut [u8]) -> crate::Result<()>,
    {
        let capacity = self.capacity;
        let refcell = self.producers.get().unwrap();
        let mut producer = refcell.0.borrow_mut_or_yield();

        // For deletion we do not specify the key length,
        // it's in the remaining bytes.
        let operation = DbOperation { database, key_length: None };
        let payload_header = EntryHeader::DbOperation(operation);
        let total_length = EntryHeader::total_key_size(key_length);
        if total_length > capacity {
            panic!("The entry is larger ({total_length} bytes) than the BBQueue capacity ({capacity} bytes)");
        }

        // Spin loop to have a frame the size we requested.
        reserve_and_write_grant(
            &mut producer,
            total_length,
            &self.sender,
            &self.sent_messages_attempts,
            &self.blocking_sent_messages_attempts,
            |grant| {
                let header_size = payload_header.header_size();
                let (header_bytes, remaining) = grant.split_at_mut(header_size);
                payload_header.serialize_into(header_bytes);
                key_writer(remaining)
            },
        )?;

        Ok(())
    }
}

/// Try to reserve a frame grant of `total_length` by spin
/// looping on the BBQueue buffer, panics if the receiver
/// has been disconnected or send a WakeUp message if necessary.
fn reserve_and_write_grant<F>(
    producer: &mut FrameProducer,
    total_length: usize,
    sender: &flume::Sender<ReceiverAction>,
    sent_messages_attempts: &AtomicUsize,
    blocking_sent_messages_attempts: &AtomicUsize,
    f: F,
) -> crate::Result<()>
where
    F: FnOnce(&mut [u8]) -> crate::Result<()>,
{
    loop {
        // An attempt means trying multiple times
        // and succeeded to send or not.
        sent_messages_attempts.fetch_add(1, atomic::Ordering::Relaxed);

        for _ in 0..10_000 {
            match producer.grant(total_length) {
                Ok(mut grant) => {
                    // We could commit only the used memory.
                    f(&mut grant)?;
                    grant.commit(total_length);

                    // We only send a wake up message when the channel is empty
                    // so that we don't fill the channel with too many WakeUps.
                    if sender.is_empty() {
                        sender.send(ReceiverAction::WakeUp).unwrap();
                    }

                    return Ok(());
                }
                Err(bbqueue::Error::InsufficientSize) => continue,
                Err(e) => unreachable!("{e:?}"),
            }
        }
        if sender.is_disconnected() {
            return Err(Error::InternalError(InternalError::AbortedIndexation));
        }

        // We made an attempt to send a message in the
        // bbqueue channel but it didn't succeeded.
        blocking_sent_messages_attempts.fetch_add(1, atomic::Ordering::Relaxed);

        // We prefer to yield and allow the writing thread
        // to do its job, especially beneficial when there
        // is only one CPU core available.
        std::thread::yield_now();
    }
}

pub enum ExactWordDocids {}
pub enum FidWordCountDocids {}
pub enum WordDocids {}
pub enum WordFidDocids {}
pub enum WordPairProximityDocids {}
pub enum WordPositionDocids {}

pub trait DatabaseType {
    const DATABASE: Database;
}

impl DatabaseType for ExactWordDocids {
    const DATABASE: Database = Database::ExactWordDocids;
}

impl DatabaseType for FidWordCountDocids {
    const DATABASE: Database = Database::FidWordCountDocids;
}

impl DatabaseType for WordDocids {
    const DATABASE: Database = Database::WordDocids;
}

impl DatabaseType for WordFidDocids {
    const DATABASE: Database = Database::WordFidDocids;
}

impl DatabaseType for WordPairProximityDocids {
    const DATABASE: Database = Database::WordPairProximityDocids;
}

impl DatabaseType for WordPositionDocids {
    const DATABASE: Database = Database::WordPositionDocids;
}

#[derive(Clone, Copy)]
pub struct WordDocidsSender<'a, 'b, D> {
    sender: &'a ExtractorBbqueueSender<'b>,
    _marker: PhantomData<D>,
}

impl<D: DatabaseType> WordDocidsSender<'_, '_, D> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);
        let key_length = key.len().try_into().ok().and_then(NonZeroU16::new).ok_or_else(|| {
            InternalError::StorePut {
                database_name: D::DATABASE.database_name(),
                key: key.into(),
                value_length,
                error: MdbError::BadValSize.into(),
            }
        })?;
        self.sender.write_key_value_with(
            D::DATABASE,
            key_length,
            value_length,
            |key_buffer, value_buffer| {
                key_buffer.copy_from_slice(key);
                CboRoaringBitmapCodec::serialize_into_writer(bitmap, value_buffer)?;
                Ok(())
            },
        )
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        self.sender.delete_entry(D::DATABASE, key)
    }
}

#[derive(Clone, Copy)]
pub struct FacetDocidsSender<'a, 'b> {
    sender: &'a ExtractorBbqueueSender<'b>,
}

impl FacetDocidsSender<'_, '_> {
    pub fn write(&self, key: &[u8], bitmap: &RoaringBitmap) -> crate::Result<()> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);

        let value_length = CboRoaringBitmapCodec::serialized_size(bitmap);
        let value_length = match facet_kind {
            // We must take the facet group size into account
            // when we serialize strings and numbers.
            FacetKind::Number | FacetKind::String => value_length + 1,
            FacetKind::Null | FacetKind::Empty | FacetKind::Exists => value_length,
        };
        let key_length = key.len().try_into().ok().and_then(NonZeroU16::new).ok_or_else(|| {
            InternalError::StorePut {
                database_name: database.database_name(),
                key: key.into(),
                value_length,
                error: MdbError::BadValSize.into(),
            }
        })?;

        self.sender.write_key_value_with(
            database,
            key_length,
            value_length,
            |key_out, value_out| {
                key_out.copy_from_slice(key);

                let value_out = match facet_kind {
                    // We must take the facet group size into account
                    // when we serialize strings and numbers.
                    FacetKind::String | FacetKind::Number => {
                        let (first, remaining) = value_out.split_first_mut().unwrap();
                        *first = 1;
                        remaining
                    }
                    FacetKind::Null | FacetKind::Empty | FacetKind::Exists => value_out,
                };

                CboRoaringBitmapCodec::serialize_into_writer(bitmap, value_out)?;

                Ok(())
            },
        )
    }

    pub fn delete(&self, key: &[u8]) -> crate::Result<()> {
        let (facet_kind, key) = FacetKind::extract_from_key(key);
        let database = Database::from(facet_kind);
        self.sender.delete_entry(database, key)
    }
}

#[derive(Clone, Copy)]
pub struct FieldIdDocidFacetSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl FieldIdDocidFacetSender<'_, '_> {
    pub fn write_facet_string(&self, key: &[u8], value: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetStringCodec::bytes_decode(key).is_ok());
        self.0.write_key_value(Database::FieldIdDocidFacetStrings, key, value)
    }

    pub fn write_facet_f64(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetF64Codec::bytes_decode(key).is_ok());
        self.0.write_key_value(Database::FieldIdDocidFacetF64s, key, &[])
    }

    pub fn delete_facet_string(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetStringCodec::bytes_decode(key).is_ok());
        self.0.delete_entry(Database::FieldIdDocidFacetStrings, key)
    }

    pub fn delete_facet_f64(&self, key: &[u8]) -> crate::Result<()> {
        debug_assert!(FieldDocIdFacetF64Codec::bytes_decode(key).is_ok());
        self.0.delete_entry(Database::FieldIdDocidFacetF64s, key)
    }
}

#[derive(Clone, Copy)]
pub struct DocumentsSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl DocumentsSender<'_, '_> {
    /// TODO do that efficiently
    pub fn uncompressed(
        &self,
        docid: DocumentId,
        external_id: String,
        document: &KvReaderFieldId,
    ) -> crate::Result<()> {
        self.0.write_key_value(Database::Documents, &docid.to_be_bytes(), document.as_bytes())?;
        self.0.write_key_value(
            Database::ExternalDocumentsIds,
            external_id.as_bytes(),
            &docid.to_be_bytes(),
        )
    }

    pub fn delete(&self, docid: DocumentId, external_id: String) -> crate::Result<()> {
        self.0.delete_entry(Database::Documents, &docid.to_be_bytes())?;
        self.0.delete_vector(docid)?;
        self.0.delete_entry(Database::ExternalDocumentsIds, external_id.as_bytes())
    }
}

#[derive(Clone, Copy)]
pub struct EmbeddingSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl EmbeddingSender<'_, '_> {
    pub fn set_vectors(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embeddings: Vec<Embedding>,
    ) -> crate::Result<()> {
        self.0.set_vectors(docid, embedder_id, &embeddings[..])
    }

    pub fn set_vector(
        &self,
        docid: DocumentId,
        embedder_id: u8,
        embedding: Embedding,
    ) -> crate::Result<()> {
        self.0.set_vectors(docid, embedder_id, &[embedding])
    }
}

#[derive(Clone, Copy)]
pub struct GeoSender<'a, 'b>(&'a ExtractorBbqueueSender<'b>);

impl GeoSender<'_, '_> {
    pub fn set_rtree(&self, value: Mmap) -> StdResult<(), SendError<()>> {
        self.0
            .sender
            .send(ReceiverAction::LargeEntry(LargeEntry {
                database: Database::Main,
                key: GEO_RTREE_KEY.to_string().into_bytes().into_boxed_slice(),
                value,
            }))
            .map_err(|_| SendError(()))
    }

    pub fn set_geo_faceted(&self, bitmap: &RoaringBitmap) -> crate::Result<()> {
        let database = Database::Main;
        let value_length = bitmap.serialized_size();
        let key = GEO_FACETED_DOCUMENTS_IDS_KEY.as_bytes();
        let key_length = key.len().try_into().ok().and_then(NonZeroU16::new).ok_or_else(|| {
            InternalError::StorePut {
                database_name: database.database_name(),
                key: key.into(),
                value_length,
                error: MdbError::BadValSize.into(),
            }
        })?;

        self.0.write_key_value_with(
            database,
            key_length,
            value_length,
            |key_buffer, value_buffer| {
                key_buffer.copy_from_slice(key);
                bitmap.serialize_into(value_buffer)?;
                Ok(())
            },
        )
    }
}
