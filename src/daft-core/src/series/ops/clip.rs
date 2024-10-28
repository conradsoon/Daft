use common_error::{DaftError, DaftResult};
use daft_schema::prelude::*;

use crate::{
    datatypes::InferDataType,
    series::{IntoSeries, Series},
};

impl Series {
    pub fn binary_min(&self, rhs: &Self) -> DaftResult<Self> {
        let (_, _, output_type) = InferDataType::from(self.data_type())
            .comparison_op(&InferDataType::from(rhs.data_type()))?;

        match &output_type {
            DataType::Int8 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i8()?.min(rhs_casted.i8()?)?.into_series())
            }
            DataType::Int16 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i16()?.min(rhs_casted.i16()?)?.into_series())
            }
            DataType::Int32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i32()?.min(rhs_casted.i32()?)?.into_series())
            }
            DataType::Int64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i64()?.min(rhs_casted.i64()?)?.into_series())
            }
            DataType::UInt8 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u8()?.min(rhs_casted.u8()?)?.into_series())
            }
            DataType::UInt16 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u16()?.min(rhs_casted.u16()?)?.into_series())
            }
            DataType::UInt32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u32()?.min(rhs_casted.u32()?)?.into_series())
            }
            DataType::UInt64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u64()?.min(rhs_casted.u64()?)?.into_series())
            }
            DataType::Float32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.f32()?.min(rhs_casted.f32()?)?.into_series())
            }
            DataType::Float64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.f64()?.min(rhs_casted.f64()?)?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "min not implemented for {}",
                dt
            ))),
        }
    }

    pub fn binary_max(&self, rhs: &Self) -> DaftResult<Self> {
        let (_, _, output_type) = InferDataType::from(self.data_type())
            .comparison_op(&InferDataType::from(rhs.data_type()))?;

        match &output_type {
            DataType::Int8 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i8()?.max(rhs_casted.i8()?)?.into_series())
            }
            DataType::Int16 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i16()?.max(rhs_casted.i16()?)?.into_series())
            }
            DataType::Int32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i32()?.max(rhs_casted.i32()?)?.into_series())
            }
            DataType::Int64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.i64()?.max(rhs_casted.i64()?)?.into_series())
            }
            DataType::UInt8 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u8()?.max(rhs_casted.u8()?)?.into_series())
            }
            DataType::UInt16 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u16()?.max(rhs_casted.u16()?)?.into_series())
            }
            DataType::UInt32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u32()?.max(rhs_casted.u32()?)?.into_series())
            }
            DataType::UInt64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.u64()?.max(rhs_casted.u64()?)?.into_series())
            }
            DataType::Float32 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.f32()?.max(rhs_casted.f32()?)?.into_series())
            }
            DataType::Float64 => {
                let lhs_casted = self.cast(&output_type)?;
                let rhs_casted = rhs.cast(&output_type)?;
                Ok(lhs_casted.f64()?.max(rhs_casted.f64()?)?.into_series())
            }
            dt => Err(DaftError::TypeError(format!(
                "max not implemented for {}",
                dt
            ))),
        }
    }

    pub fn clip(&self, min: &Self, max: &Self) -> DaftResult<Self> {
        // We follow numpy's semantics in defining clip (equivalent to np.minimum(a_max, np.maximum(a, a_min)).
        // NOTE: As per numpy, this **doesn't** throw an error if max < min unlike the std::clamp function, it just returns an array that's entirely a_max.
        self.binary_max(min)?.binary_min(max)
    }
}
