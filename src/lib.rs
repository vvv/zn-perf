mod error;

pub use error::{ZnError, ZnResult};

pub mod file {
    use crate::ZnResult;
    use memchr::memmem;
    use parquet::{
        basic::Type as BasicType,
        file::{metadata::ParquetMetaData, reader::FileReader},
        record::Field,
        schema::types::Type as SchemaType,
    };

    pub fn read_all_data<R: FileReader>(file_reader: &R) -> ZnResult<()> {
        let mut row_iter = file_reader.get_row_iter(None)?;
        assert!(row_iter.all(|row| {
            // Consume the row by converting it into JSON value. Then consume the
            // JSON value by comparing it with `null`.
            !row.to_json_value().is_null()
        }));
        Ok(())
    }

    fn is_byte_array(t: BasicType) -> bool {
        matches!(t, BasicType::BYTE_ARRAY | BasicType::FIXED_LEN_BYTE_ARRAY)
    }

    /// Returns the projection of [byte array] columns.
    ///
    /// [byte_array]: is_byte_array()
    fn byte_array_columns(metadata: &ParquetMetaData) -> SchemaType {
        match metadata.file_metadata().schema().clone() {
            SchemaType::PrimitiveType { .. } => unimplemented!(),
            SchemaType::GroupType {
                basic_info,
                mut fields,
            } => {
                fields.retain(|t| is_byte_array(t.get_physical_type()));
                SchemaType::GroupType { basic_info, fields }
            }
        }
    }

    /// Returns total byte size of uncompressed data of all [byte array] columns.
    ///
    /// [byte_array]: is_byte_array()
    pub fn byte_array_columns_uncompressed_size(metadata: &ParquetMetaData) -> u64 {
        let mut size = 0;
        for row_group in metadata.row_groups() {
            size += row_group
                .columns()
                .iter()
                .filter_map(|col| is_byte_array(col.column_type()).then(|| col.uncompressed_size()))
                .sum::<i64>();
        }
        size.try_into().expect("BUG")
    }

    /// Counts the occurrences of `needle` in all `BYTE ARRAY` columns of the
    /// `haystack`.
    ///
    /// # Panics
    ///
    /// Panics if `needle` is empty.
    pub fn count_occurrences<R: FileReader>(haystack: &R, needle: &[u8]) -> ZnResult<usize> {
        assert!(!needle.is_empty());

        let projection = byte_array_columns(haystack.metadata());
        let row_iter = haystack.get_row_iter(Some(projection))?;
        let mut count = 0;
        for row in row_iter {
            for (_column_name, value) in row.get_column_iter() {
                match value {
                    Field::Null => (),
                    Field::Bool(_) => todo!(),
                    Field::Byte(_) => todo!(),
                    Field::Short(_) => todo!(),
                    Field::Int(_) => todo!(),
                    Field::Long(_) => todo!(),
                    Field::UByte(_) => todo!(),
                    Field::UShort(_) => todo!(),
                    Field::UInt(_) => todo!(),
                    Field::ULong(_) => todo!(),
                    Field::Float(_) => todo!(),
                    Field::Double(_) => todo!(),
                    Field::Decimal(_) => todo!(),
                    // Field::Str(s) => count += memmem::find_iter(s.as_bytes(), needle).count(),
                    Field::Str(s) => {
                        if memmem::find(s.as_bytes(), needle).is_some() {
                            count += 1;
                            // We know that this row contains the needle. There is
                            // no need to scan it any further.
                            break;
                        }
                    }
                    Field::Bytes(_) => todo!(),
                    Field::Date(_) => todo!(),
                    Field::TimestampMillis(_) => todo!(),
                    Field::TimestampMicros(_) => todo!(),
                    Field::Group(_) => todo!(),
                    Field::ListInternal(_) => todo!(),
                    Field::MapInternal(_) => todo!(),
                }
            }
        }
        Ok(count)
    }
}
