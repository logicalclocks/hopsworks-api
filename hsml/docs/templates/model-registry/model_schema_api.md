# Model Schema

## Creation

To create a ModelSchema, the schema of the Model inputs and/or Model ouputs has to be defined beforehand.

{{schema}}

After defining the Model inputs and/or outputs schemas, a ModelSchema can be created using its class constructor.

{{model_schema}}

## Retrieval

### Model Schema

Model schemas can be accessed from the model metadata objects.

``` python
model.model_schema
```

### Model Input & Ouput Schemas

The schemas of the Model inputs and outputs can be accessed from the ModelSchema metadata objects.

``` python
model_schema.input_schema
model_schema.output_schema
```

## Methods

{{schema_dict}}

{{model_schema_dict}}
