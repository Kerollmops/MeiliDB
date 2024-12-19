use std::cell::RefCell;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Seek as _, Write as _};
use std::{iter, mem, result};

use bumpalo::Bump;
use bytemuck::{bytes_of, pod_read_unaligned, Pod, Zeroable};
use heed::RoTxn;
use serde_json::value::RawValue;
use serde_json::Value;

use crate::error::GeoError;
use crate::update::new::document::Document;
use crate::update::new::indexer::document_changes::{DocumentChangeContext, Extractor};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::thread_local::MostlySend;
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{lat_lng_to_xyz, DocumentId, GeoPoint, Index, InternalError, Result};

pub struct GeoExtractor {
    grenad_parameters: GrenadParameters,
}

impl GeoExtractor {
    pub fn new(
        rtxn: &RoTxn,
        index: &Index,
        grenad_parameters: GrenadParameters,
    ) -> Result<Option<Self>> {
        let is_sortable = index.sortable_fields(rtxn)?.contains("_geo");
        let is_filterable = index.filterable_fields(rtxn)?.contains("_geo");
        if is_sortable || is_filterable {
            Ok(Some(GeoExtractor { grenad_parameters }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C, packed)]
pub struct ExtractedGeoPoint {
    pub docid: DocumentId,
    pub lat_lng: [f64; 2],
}

impl From<ExtractedGeoPoint> for GeoPoint {
    /// Converts the latitude and longitude back to an xyz GeoPoint.
    fn from(value: ExtractedGeoPoint) -> Self {
        let [lat, lng] = value.lat_lng;
        let point = [lat, lng];
        let xyz_point = lat_lng_to_xyz(&point);
        GeoPoint::new(xyz_point, (value.docid, point))
    }
}

pub struct GeoExtractorData<'extractor> {
    /// The set of documents ids that were removed. If a document sees its geo
    /// point being updated, we first put it in the deleted and then in the inserted.
    removed: bumpalo::collections::Vec<'extractor, ExtractedGeoPoint>,
    inserted: bumpalo::collections::Vec<'extractor, ExtractedGeoPoint>,
    /// Contains a packed list of `ExtractedGeoPoint` of the inserted geo points
    /// data structures if we have spilled to disk.
    spilled_removed: Option<BufWriter<File>>,
    /// Contains a packed list of `ExtractedGeoPoint` of the inserted geo points
    /// data structures if we have spilled to disk.
    spilled_inserted: Option<BufWriter<File>>,
}

impl<'extractor> GeoExtractorData<'extractor> {
    pub fn freeze(self) -> Result<FrozenGeoExtractorData<'extractor>> {
        let GeoExtractorData { removed, inserted, spilled_removed, spilled_inserted } = self;

        Ok(FrozenGeoExtractorData {
            removed: removed.into_bump_slice(),
            inserted: inserted.into_bump_slice(),
            spilled_removed: spilled_removed
                .map(|bw| bw.into_inner().map(BufReader::new).map_err(|iie| iie.into_error()))
                .transpose()?,
            spilled_inserted: spilled_inserted
                .map(|bw| bw.into_inner().map(BufReader::new).map_err(|iie| iie.into_error()))
                .transpose()?,
        })
    }
}

unsafe impl MostlySend for GeoExtractorData<'_> {}

pub struct FrozenGeoExtractorData<'extractor> {
    pub removed: &'extractor [ExtractedGeoPoint],
    pub inserted: &'extractor [ExtractedGeoPoint],
    pub spilled_removed: Option<BufReader<File>>,
    pub spilled_inserted: Option<BufReader<File>>,
}

impl<'extractor> FrozenGeoExtractorData<'extractor> {
    pub fn iter_and_clear_removed(
        &mut self,
    ) -> io::Result<impl IntoIterator<Item = io::Result<ExtractedGeoPoint>> + '_> {
        Ok(mem::take(&mut self.removed)
            .iter()
            .copied()
            .map(Ok)
            .chain(iterator_over_spilled_geopoints(&mut self.spilled_removed)?))
    }

    pub fn iter_and_clear_inserted(
        &mut self,
    ) -> io::Result<impl IntoIterator<Item = io::Result<ExtractedGeoPoint>> + '_> {
        Ok(mem::take(&mut self.inserted)
            .iter()
            .copied()
            .map(Ok)
            .chain(iterator_over_spilled_geopoints(&mut self.spilled_inserted)?))
    }
}

fn iterator_over_spilled_geopoints(
    spilled: &mut Option<BufReader<File>>,
) -> io::Result<impl IntoIterator<Item = io::Result<ExtractedGeoPoint>> + '_> {
    let mut spilled = spilled.take();
    if let Some(spilled) = &mut spilled {
        spilled.rewind()?;
    }

    Ok(iter::from_fn(move || match &mut spilled {
        Some(file) => {
            let geopoint_bytes = &mut [0u8; mem::size_of::<ExtractedGeoPoint>()];
            match file.read_exact(geopoint_bytes) {
                Ok(()) => Some(Ok(pod_read_unaligned(geopoint_bytes))),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
                Err(e) => Some(Err(e)),
            }
        }
        None => None,
    }))
}

impl<'extractor> Extractor<'extractor> for GeoExtractor {
    type Data = RefCell<GeoExtractorData<'extractor>>;

    fn init_data<'doc>(&'doc self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(GeoExtractorData {
            removed: bumpalo::collections::Vec::new_in(extractor_alloc),
            inserted: bumpalo::collections::Vec::new_in(extractor_alloc),
            spilled_inserted: None,
            spilled_removed: None,
        }))
    }

    fn process<'doc>(
        &'doc self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &'doc DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        let rtxn = &context.rtxn;
        let index = context.index;
        let max_memory = self.grenad_parameters.max_memory_by_thread();
        let db_fields_ids_map = context.db_fields_ids_map;
        let db_document_decompression_dictionary = context.db_document_decompression_dictionary;
        let doc_alloc = &context.doc_alloc;
        let mut data_ref = context.data.borrow_mut_or_yield();

        for change in changes {
            if data_ref.spilled_removed.is_none()
                && max_memory.map_or(false, |mm| context.extractor_alloc.allocated_bytes() >= mm)
            {
                // We must spill as we allocated too much memory
                data_ref.spilled_removed = tempfile::tempfile().map(BufWriter::new).map(Some)?;
                data_ref.spilled_inserted = tempfile::tempfile().map(BufWriter::new).map(Some)?;
            }

            match change? {
                DocumentChange::Deletion(deletion) => {
                    let docid = deletion.docid();
                    let external_id = deletion.external_document_id();
                    let current = deletion.current(
                        rtxn,
                        index,
                        db_fields_ids_map,
                        db_document_decompression_dictionary,
                        doc_alloc,
                    )?;
                    let current_geo = current
                        .geo_field()?
                        .map(|geo| extract_geo_coordinates(external_id, geo))
                        .transpose()?;

                    if let Some(lat_lng) = current_geo.flatten() {
                        let geopoint = ExtractedGeoPoint { docid, lat_lng };
                        match &mut data_ref.spilled_removed {
                            Some(file) => file.write_all(bytes_of(&geopoint))?,
                            None => data_ref.removed.push(geopoint),
                        }
                    }
                }
                DocumentChange::Update(update) => {
                    let current = update.current(
                        rtxn,
                        index,
                        db_fields_ids_map,
                        db_document_decompression_dictionary,
                        doc_alloc,
                    )?;
                    let external_id = update.external_document_id();
                    let docid = update.docid();

                    let current_geo = current
                        .geo_field()?
                        .map(|geo| extract_geo_coordinates(external_id, geo))
                        .transpose()?;

                    let updated_geo = update
                        .updated()
                        .geo_field()?
                        .map(|geo| extract_geo_coordinates(external_id, geo))
                        .transpose()?;

                    if current_geo != updated_geo {
                        // If the current and new geo points are different it means that
                        // we need to replace the current by the new point and therefore
                        // delete the current point from the RTree.
                        if let Some(lat_lng) = current_geo.flatten() {
                            let geopoint = ExtractedGeoPoint { docid, lat_lng };
                            match &mut data_ref.spilled_removed {
                                Some(file) => file.write_all(bytes_of(&geopoint))?,
                                None => data_ref.removed.push(geopoint),
                            }
                        }

                        if let Some(lat_lng) = updated_geo.flatten() {
                            let geopoint = ExtractedGeoPoint { docid, lat_lng };
                            match &mut data_ref.spilled_inserted {
                                Some(file) => file.write_all(bytes_of(&geopoint))?,
                                None => data_ref.inserted.push(geopoint),
                            }
                        }
                    }
                }
                DocumentChange::Insertion(insertion) => {
                    let external_id = insertion.external_document_id();
                    let docid = insertion.docid();

                    let inserted_geo = insertion
                        .inserted()
                        .geo_field()?
                        .map(|geo| extract_geo_coordinates(external_id, geo))
                        .transpose()?;

                    if let Some(lat_lng) = inserted_geo.flatten() {
                        let geopoint = ExtractedGeoPoint { docid, lat_lng };
                        match &mut data_ref.spilled_inserted {
                            Some(file) => file.write_all(bytes_of(&geopoint))?,
                            None => data_ref.inserted.push(geopoint),
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Extracts and validates the latitude and latitude from a document geo field.
///
/// It can be of the form `{ "lat": 0.0, "lng": "1.0" }`.
pub fn extract_geo_coordinates(
    external_id: &str,
    raw_value: &RawValue,
) -> Result<Option<[f64; 2]>> {
    let mut geo = match serde_json::from_str(raw_value.get()).map_err(InternalError::SerdeJson)? {
        Value::Null => return Ok(None),
        Value::Object(map) => map,
        value => {
            return Err(
                GeoError::NotAnObject { document_id: Value::from(external_id), value }.into()
            )
        }
    };

    let [lat, lng] = match (geo.remove("lat"), geo.remove("lng")) {
        (Some(lat), Some(lng)) => {
            if geo.is_empty() {
                [lat, lng]
            } else {
                return Err(GeoError::UnexpectedExtraFields {
                    document_id: Value::from(external_id),
                    value: Value::from(geo),
                }
                .into());
            }
        }
        (Some(_), None) => {
            return Err(GeoError::MissingLongitude { document_id: Value::from(external_id) }.into())
        }
        (None, Some(_)) => {
            return Err(GeoError::MissingLatitude { document_id: Value::from(external_id) }.into())
        }
        (None, None) => {
            return Err(GeoError::MissingLatitudeAndLongitude {
                document_id: Value::from(external_id),
            }
            .into())
        }
    };

    match (extract_finite_float_from_value(lat), extract_finite_float_from_value(lng)) {
        (Ok(lat), Ok(lng)) => Ok(Some([lat, lng])),
        (Ok(_), Err(value)) => {
            Err(GeoError::BadLongitude { document_id: Value::from(external_id), value }.into())
        }
        (Err(value), Ok(_)) => {
            Err(GeoError::BadLatitude { document_id: Value::from(external_id), value }.into())
        }
        (Err(lat), Err(lng)) => Err(GeoError::BadLatitudeAndLongitude {
            document_id: Value::from(external_id),
            lat,
            lng,
        }
        .into()),
    }
}

/// Extracts and validate that a serde JSON Value is actually a finite f64.
pub fn extract_finite_float_from_value(value: Value) -> result::Result<f64, Value> {
    let number = match value {
        Value::Number(ref n) => match n.as_f64() {
            Some(number) => number,
            None => return Err(value),
        },
        Value::String(ref s) => match s.parse::<f64>() {
            Ok(number) => number,
            Err(_) => return Err(value),
        },
        value => return Err(value),
    };

    if number.is_finite() {
        Ok(number)
    } else {
        Err(value)
    }
}
