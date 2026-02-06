# Login API

::: hopsworks.login
    options:
      show_root_full_path: true

::: hopsworks.get_current_project
    options:
      show_root_full_path: true

## Specialized APIs

Once you obtain a project using one of the above methods, you can use the specialized APIs available on the project object.
For example: [get_feature_store][hopsworks_common.project.Project.get_feature_store], [get_model_registry][hopsworks_common.project.Project.get_model_registry], [get_model_serving][hopsworks_common.project.Project.get_model_serving], etc.
