use common_error::{DaftError, DaftResult};
use daft_core::{
    datatypes::InferDataType,
    prelude::{Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{ScalarFunction, ScalarUDF},
    ExprRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryMin;

#[typetag::serde]
impl ScalarUDF for BinaryMin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "binary_min"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 2 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arguments for 'binary_min', got {}",
                inputs.len()
            )));
        }
        let lhs_field = inputs[0].to_field(schema)?;
        let rhs_field = inputs[1].to_field(schema)?;

        let lhs_dtype = &lhs_field.dtype;
        let rhs_dtype = &rhs_field.dtype;

        if !lhs_dtype.is_numeric() || !rhs_dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "All inputs to 'binary_min' must be numeric types, got {:?} and {:?}",
                lhs_dtype, rhs_dtype
            )));
        }

        let lhs_infer = InferDataType::from(lhs_dtype);
        let rhs_infer = InferDataType::from(rhs_dtype);

        let (_, _, output_type) = lhs_infer.comparison_op(&rhs_infer)?;

        Ok(Field::new(lhs_field.name.clone(), output_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 2 {
            return Err(DaftError::ValueError(format!(
                "Expected 2 input arguments for 'binary_min', got {}",
                inputs.len()
            )));
        }
        let lhs = &inputs[0];
        let rhs = &inputs[1];

        lhs.binary_min(rhs)
    }
}

#[must_use]
pub fn binary_min(lhs: ExprRef, rhs: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryMin, vec![lhs, rhs]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BinaryMax;

#[typetag::serde]
impl ScalarUDF for BinaryMax {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "binary_max"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 2 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 2 input arguments for 'binary_max', got {}",
                inputs.len()
            )));
        }
        let lhs_field = inputs[0].to_field(schema)?;
        let rhs_field = inputs[1].to_field(schema)?;

        let lhs_dtype = &lhs_field.dtype;
        let rhs_dtype = &rhs_field.dtype;

        if !lhs_dtype.is_numeric() || !rhs_dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "All inputs to 'binary_max' must be numeric types, got {:?} and {:?}",
                lhs_dtype, rhs_dtype
            )));
        }

        let lhs_infer = InferDataType::from(lhs_dtype);
        let rhs_infer = InferDataType::from(rhs_dtype);

        let (_, _, output_type) = lhs_infer.comparison_op(&rhs_infer)?;

        Ok(Field::new(lhs_field.name.clone(), output_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 2 {
            return Err(DaftError::ValueError(format!(
                "Expected 2 input arguments for 'binary_max', got {}",
                inputs.len()
            )));
        }
        let lhs = &inputs[0];
        let rhs = &inputs[1];

        lhs.binary_max(rhs)
    }
}

#[must_use]
pub fn binary_max(lhs: ExprRef, rhs: ExprRef) -> ExprRef {
    ScalarFunction::new(BinaryMax, vec![lhs, rhs]).into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Clip;

#[typetag::serde]
impl ScalarUDF for Clip {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "clip"
    }

    fn to_field(&self, inputs: &[ExprRef], schema: &Schema) -> DaftResult<Field> {
        if inputs.len() != 3 {
            return Err(DaftError::SchemaMismatch(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
        let array_field = inputs[0].to_field(schema)?;
        let min_field = inputs[1].to_field(schema)?;
        let max_field = inputs[2].to_field(schema)?;

        let array_dtype = &array_field.dtype;
        let min_dtype = &min_field.dtype;
        let max_dtype = &max_field.dtype;

        // Ensure that the input types are numeric
        if !array_dtype.is_numeric() || !min_dtype.is_numeric() || !max_dtype.is_numeric() {
            return Err(DaftError::TypeError(format!(
                "All inputs to 'clip' must be numeric types, got {:?}, {:?}, {:?}",
                array_dtype, min_dtype, max_dtype
            )));
        }

        // Determine the common output type
        let array_infer = InferDataType::from(array_dtype);
        let min_infer = InferDataType::from(min_dtype);
        let max_infer = InferDataType::from(max_dtype);

        let (_, _, intermediate_type) = array_infer.comparison_op(&min_infer)?;
        let intermediate_infer = InferDataType::from(&intermediate_type);
        let (_, _, output_type) = max_infer.comparison_op(&intermediate_infer)?;

        // Convert `InferDataType` back to `DataType`
        Ok(Field::new(array_field.name.clone(), output_type))
    }

    fn evaluate(&self, inputs: &[Series]) -> DaftResult<Series> {
        if inputs.len() != 3 {
            return Err(DaftError::ValueError(format!(
                "Expected 3 input arguments (array, min, max), got {}",
                inputs.len()
            )));
        }
        let array = &inputs[0];
        let min = &inputs[1];
        let max = &inputs[2];

        array.clip(min, max)
    }
}

#[must_use]
pub fn clip(array: ExprRef, min: ExprRef, max: ExprRef) -> ExprRef {
    ScalarFunction::new(Clip, vec![array, min, max]).into()
}

#[cfg(feature = "python")]
use {
    daft_dsl::python::PyExpr,
    pyo3::{pyfunction, PyResult},
};

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "binary_min")]
pub fn py_binary_min(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
    Ok(binary_min(lhs.into(), rhs.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "binary_max")]
pub fn py_binary_max(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
    Ok(binary_max(lhs.into(), rhs.into()).into())
}

#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(name = "clip")]
pub fn py_clip(array: PyExpr, min: PyExpr, max: PyExpr) -> PyResult<PyExpr> {
    Ok(clip(array.into(), min.into(), max.into()).into())
}
