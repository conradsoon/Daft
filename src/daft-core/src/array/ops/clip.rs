use common_error::DaftResult;

use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, Float32Type, Float64Type},
    prelude::DaftIntegerType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType + DaftIntegerType, // Need the DaftIntegerType to tell the compiler that this doesn't apply for Float32/Float64, so we can specialize the implementation
    T::Native: Ord,
{
    pub fn min(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.min(r))
    }
    pub fn max(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.max(r))
    }
}

// Ideally, I'd like to further specialize the template to use the float's min/max version,
// but I am too dumb for now to figure out Rust's type system, so let's just do this for now.

impl DataArray<Float32Type> {
    pub fn min(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.min(r))
    }
    pub fn max(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.max(r))
    }
}

impl DataArray<Float64Type> {
    pub fn min(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.min(r))
    }
    pub fn max(&self, rhs: &Self) -> DaftResult<Self> {
        self.binary_apply(rhs, |l, r| l.max(r))
    }
}
