use std::collections::HashSet;

use serde::Serialize;
use serde::ser;

use crate::database::serde::indexer_serializer::IndexerSerializer;
use crate::database::serde::key_to_string::KeyToStringSerializer;
use crate::database::serde::value_to_number::ValueToNumberSerializer;
use crate::database::update::DocumentUpdate;
use crate::database::serde::SerializerError;
use crate::database::schema::Schema;
use meilidb_core::DocumentId;

pub struct Serializer<'a, 'b> {
    pub schema: &'a Schema,
    pub update: &'a mut DocumentUpdate<'b>,
    pub document_id: DocumentId,
    pub stop_words: &'a HashSet<String>,
}

impl<'a, 'b> ser::Serializer for Serializer<'a, 'b> {
    type Ok = ();
    type Error = SerializerError;
    type SerializeSeq = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = MapSerializer<'a, 'b>;
    type SerializeStruct = StructSerializer<'a, 'b>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    forward_to_unserializable_type! {
        bool => serialize_bool,
        char => serialize_char,

        i8  => serialize_i8,
        i16 => serialize_i16,
        i32 => serialize_i32,
        i64 => serialize_i64,

        u8  => serialize_u8,
        u16 => serialize_u16,
        u32 => serialize_u32,
        u64 => serialize_u64,

        f32 => serialize_f32,
        f64 => serialize_f64,
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "str" })
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "&[u8]" })
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "Option" })
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "()" })
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(SerializerError::UnserializableType { name: "unit struct" })
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str
    ) -> Result<Self::Ok, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "unit variant" })
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T
    ) -> Result<Self::Ok, Self::Error>
    where T: Serialize,
    {
        Err(SerializerError::UnserializableType { name: "newtype variant" })
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(SerializerError::UnserializableType { name: "sequence" })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(SerializerError::UnserializableType { name: "tuple" })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleStruct, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple struct" })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeTupleVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "tuple variant" })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(MapSerializer {
            schema: self.schema,
            document_id: self.document_id,
            update: self.update,
            stop_words: self.stop_words,
            current_key_name: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStruct, Self::Error>
    {
        Ok(StructSerializer {
            schema: self.schema,
            document_id: self.document_id,
            update: self.update,
            stop_words: self.stop_words,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize
    ) -> Result<Self::SerializeStructVariant, Self::Error>
    {
        Err(SerializerError::UnserializableType { name: "struct variant" })
    }
}

pub struct MapSerializer<'a, 'b> {
    pub schema: &'a Schema,
    pub document_id: DocumentId,
    pub update: &'a mut DocumentUpdate<'b>,
    pub stop_words: &'a HashSet<String>,
    pub current_key_name: Option<String>,
}

impl<'a, 'b> ser::SerializeMap for MapSerializer<'a, 'b> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where T: Serialize,
    {
        let key = key.serialize(KeyToStringSerializer)?;
        self.current_key_name = Some(key);
        Ok(())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where T: Serialize,
    {
        let key = self.current_key_name.take().unwrap();
        self.serialize_entry(&key, value)
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where K: Serialize, V: Serialize,
    {
        let key = key.serialize(KeyToStringSerializer)?;

        if let Some(attr) = self.schema.attribute(key) {
            let props = self.schema.props(attr);
            if props.is_stored() {
                let value = bincode::serialize(value).unwrap();
                self.update.insert_attribute_value(attr, &value)?;
            }
            if props.is_indexed() {
                let serializer = IndexerSerializer {
                    update: self.update,
                    document_id: self.document_id,
                    attribute: attr,
                    stop_words: self.stop_words,
                };
                value.serialize(serializer)?;
            }
            if props.is_ranked() {
                let number = value.serialize(ValueToNumberSerializer)?;
                self.update.register_ranked_attribute(attr, number)?;
            }
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub struct StructSerializer<'a, 'b> {
    pub schema: &'a Schema,
    pub document_id: DocumentId,
    pub update: &'a mut DocumentUpdate<'b>,
    pub stop_words: &'a HashSet<String>,
}

impl<'a, 'b> ser::SerializeStruct for StructSerializer<'a, 'b> {
    type Ok = ();
    type Error = SerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T
    ) -> Result<(), Self::Error>
    where T: Serialize,
    {
        if let Some(attr) = self.schema.attribute(key) {
            let props = self.schema.props(attr);
            if props.is_stored() {
                let value = bincode::serialize(value).unwrap();
                self.update.insert_attribute_value(attr, &value)?;
            }
            if props.is_indexed() {
                let serializer = IndexerSerializer {
                    update: self.update,
                    document_id: self.document_id,
                    attribute: attr,
                    stop_words: self.stop_words,
                };
                value.serialize(serializer)?;
            }
            if props.is_ranked() {
                let integer = value.serialize(ValueToNumberSerializer)?;
                self.update.register_ranked_attribute(attr, integer)?;
            }
        }

        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
