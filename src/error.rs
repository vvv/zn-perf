use thiserror::Error;

pub type ZnResult<T, E = ZnError> = Result<T, E>;

#[derive(Debug, Error)]
pub enum ZnError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),
}
