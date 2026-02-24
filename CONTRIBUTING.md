## Python Development Setup

01. Fork and clone the repository on GitHub.

02. Create a new Python environment and install the repository in editable mode with development dependencies (we recommend to use [uv](https://docs.astral.sh/uv/)):

    ```bash
    cd python
    uv sync --extra dev --all-groups
    source .venv/bin/activate
    ```

03. The repository uses a number of automated checks and tests which you have to satisfy before your PR can be merged, see `.github/workflows/python.yml` for the exact checks.

    To run the tests locally, use `pytest`:

    ```bash
    pytest tests
    ```

    The linting and formatting are done via [ruff](https://docs.astral.sh/ruff/), see `tool.ruff` section of `pyproject.toml` for the configuration.

    To automate linting and formatting, install [`pre-commit`](https://pre-commit.com/) and then activate its hooks.
    `pre-commit` is a framework for managing and maintaining multi-language pre-commit hooks.
    Activate the git hooks with:

    ```bash
    pre-commit install
    ```

    Afterwards, `pre-commit` will run whenever you commit.
    It is not guaranteed that it will fix all linting problems.

    To run formatting and code-style separately, you can configure your IDE, such as VSCode, to use [the `ruff` extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff), or run it via the command line:

    ```bash
    # linting
    ruff check --fix
    # formatting
    ruff format
    ```

### Python API Reference Documentation

The [`hopsworks-api`](https://github.com/logicalclocks/hopsworks-api) repository contains the code of our Python API, and its reference documentation is built from the docstrings in the code.
[`hopsworks-apigen`](https://github.com/logicalclocks/hopsworks-apigen) is used to mark the public entities with its `@public` (see more in its [`README.md`](https://github.com/logicalclocks/hopsworks-apigen/blob/main/README.md)).
The rest of the documentation is located in [`logicalclocks.github.io`](https://github.com/logicalclocks/logicalclocks.github.io) repository.
To build the full docs, `hopsworks-api` is cloned into `logicalclocks.github.io` as a subdirectory and then the whole docs are built as a single website.
For the details of the building process, see [`README.md`](https://github.com/logicalclocks/logicalclocks.github.io/blob/main/README.md) of `logicalclocks.github.io`.
The whole building process is automated and is triggered on a releasing push to the release branches of `hopsworks-api` via `.github/workflows/mkdocs-release.yml` GitHub Action.

#### Docstring Guidelines

Everything public should have a docstring, following [PEP-257](https://peps.python.org/pep-0257/) and formatted using [the Google style](https://mkdocstrings.github.io/griffe/reference/docstrings/#google-style).
We use [mkdocs-material admonitions](https://squidfunk.github.io/mkdocs-material/reference/admonitions/?h=admoni#supported-types) where appropriate, which we specify using the [Google style](https://mkdocstrings.github.io/griffe/reference/docstrings/#google-admonitions) as well.
We do _not_ specify the types or defaults of parameters in the docstrings, as they are automatically extracted from the signatures by the documentation generator, so specify them in the signature.

The development version of the documentation, built from the main branches of our repositories, is hidden from the version selector but available at <https://docs.hopsworks.ai/dev/>.

##### Writing Good Docstrings

Avoid tautological docstrings.
For example, instead of:

```python
description: str
"""Description of the feature."""
```

write:

```python
description: str
"""The description of the feature as it is shown in the UI."""
```

Note how here the user gets new information about `description` which is not deducible from the name alone, concretely, that the description parameter is shown in the UI.

Always try to provide an insight or show the intent of the method or class in the docstring, instead of just repeating its name or signature.
Instead of a warning which does not really explain the reasons to be cautious, like:

```plaintext
Danger: Potentially dangerous operation
    This operation stops the execution.
```

write something like:

```plaintext
Danger: Potentially dangerous operation
    This operation kills the execution, without allowing it to shut down gracefully.
    This may result in corrupted data, which can be very hard to reverse.
```

Note how in the second example it becomes clear why exactly the operation is dangerous, and therefore the user can make an educated choice on whether to use it.
Never write "do not do ..." without explaining the reasons behind these restrictions.

Overall, think about what information the user of the API would need to know in order to use it correctly and effectively, and provide that information in the docstrings in a concise and precise manner.

##### Technical Details

Always place a sentence per line for clear git diffs.

Overall a docstrings should be structured as follows:

```python
"""Does something.

The details about the exact way something is done.
It can span multiple lines or paragraphs.
Use proper sentences for everything in the docstring except admonition titles; that is, start with a capital letter and end with a period, question or exclamation mark.

Note: Use admonitions
    Where appropriate, use admonitions to highlight important information.

After the full description, list the parameters, return values, raised exceptions, and examples.

Parameters:
    param1: A one-line description of param1.
    param2:
        A multi-line description of param2.
        In this case, the first line should be on the next line and indented.

Returns:
    A description of the return value.

Raises:
    SomeError: If something goes wrong.
"""
```

You should use `Yields` instead of `Returns` for generator functions.
Also, always use `Example` admonition instead of `Examples` section, as it will not be rendered correctly.

For example, a method docstring could look like this:

```python
def delete(self, path: str, missing_ok: bool = False) -> None:
    """Delete a file or directory at the given path.

    The path can be specified with or without the project prefix, that is, `/Projects/{project_name}`.

    Example: Ensuring a file or directory does not exist
        ```python
        import hopsworks

        project = hopsworks.login()
        project.get_dataset_api().delete("/my_tmp", missing_ok=True)
        ```

    Parameters:
        path: The path to the file or directory to delete.
        missing_ok: Whether to raise an error if the file or directory does not exist.

    Raises:
        hopsworks.client.exceptions.RestAPIError: If the server returns an error.
        FileNotFoundError: If `missing_ok` is `False` and the file or directory does not exist.
    """
    ...
```

Or this:

```python
@deprecated("hsfs.feature.Feature.isin")
# ^ Adds deprecation warning telling to use isin instead.
# See https://github.com/logicalclocks/hopsworks-apigen/blob/main/README.md
def contains(self, other: str | list) -> filter.Filter:
    """Construct a filter similar to SQL's `IN` operator.

    Parameters:
        other: A single feature value or a list of feature values.

    Returns:
        A filter that leaves only the feature values also contained in `other`.
    """
    ...
```

In case you want to deprecate an alias or an attribute, you have to use admonitions.
Always place the aliases warning last in the docstring, and parameters one before the parameters.
Hopsworks version should be specified as `major.minor`, also always say what to use instead.

```python
@public("", "hsfs.function", "hsml.function")
def function(
    *,
    old_param: str,
    new_param: str,
):
    """...

    ...

    Warning: Deprecated Parameters
        Parameter `old_param` is deprecated, and will be removed in Hopsworks v5.0.
        Use `new_param` instead.

    Parameters:
        old_param: Deprecated, use `new_param` instead.
        new param: ...

    ...

    Warning: Deprecated Aliases
        Aliases [`hsfs.function`][hsfs.function] and [`hsml.function`][hsml.function] are deprecated, and will be removed in a future release of Hopsworks.
    """
    ...
```

##### Linking

A good API reference is easy to explore, and links are essential for that.

If you mention other classes, methods, or functions in the docstring, link to them using the following syntax:

```markdown
[`ClassName`][full.module.path.ClassName]
[`ClassName.method_name`][full.module.path.ClassName.method_name]
As a convention, for methods and properties include the class name as well to reduce ambiguity.
```

Note that you can link entities defined in other libraries as well, like `pandas` or `numpy`.

To link a page of documentation, use its main header (h1) ID in square brackets:

```markdown
[Data Transformations][data-transformations]
[Hopsworks On-Premise Installation][hopsworks-on-premise-installation]
```

For external links, use the normal Markdown syntax:

```markdown
[Hopsworks Website](https://www.hopsworks.ai)
```

##### Patch Notes

The patch versions, namely Z in X.Y.Z versioning scheme, are not included into the version selector on the documentation website.
That is, it is only possible to select major and minor versions, like `3.8` or `4.4`, but not `3.8.1` or `4.4.2`.
This means that if you are adding new functionality or fixing bugs in patch releases, you should be careful with how you update the docstrings to reflect the changes.

You should make it clear from which exact version the changes are available.
The recommended way to do that is to add an info admonition block to the docstring, with a title summarizing the change and making it clear from which version it is available, like this:

```python
"""Draws a pie chart.

... The main docstring

Info: Collapsing every slice smaller than 1% into `Other`, ~=3.8.1
    If a slice is smaller than 1% of the total, it will be collapsed into a single `Other` slice together with the rest of such slices.
    This behavior is available starting from version 3.8.1 and is set as the default.
    To disable it, set the `collapse_small_slices` parameter to `False`.

Parameters:
    ...
"""
```

Or:

```python
"""Draws a pie chart.

... The main docstring

Info: Rendering charts of more than 100 slices, ~=4.4.2
    Previously, charts with more than 100 slices would fail to render with a [`SpecificException`][full.path.SpecificException].
    This has been fixed in version 4.4.2 and later.

Parameters:
    ...
"""
```

##### Summary

- Always document public classes, methods, functions, and modules.
- Show the intent and provide an insight with your docstrings, avoid tautologies.
- Do not place a warning without a proper explanation of the reasons behind it.
- Use proper sentences, starting with a capital letter and ending with a period, question or exclamation mark.
- Place a sentence per line.
- Use Google style for docstrings.
- Provide a link whenever you mention something linkable.
- Use mkdocs-material admonitions where appropriate.
- Do not duplicate information that can be extracted from the code signatures.
- Keep the documentation in the code (docstrings) as complete as possible, and avoid writing custom Markdown text in the files of the `docs` directory.
- Then making changes in patch releases, add an info admonition specifying the exact version from which the change is available.

#### Extending the API Reference

To create a new API reference page, you have to create a new markdown file in `docs` and add it to the `nav` section of the `mkdocs.yml` file:

```yaml
nav:
  - Login: login.md
  - Platform API:
    - ...
    - New Package: new_package.md
```

Inside the `new_package.md` file you can use `:::` syntax to include the documentation of different Python entities by providing their full path:

```markdown
# The New Package

::: hopsworks_common.new_package.NewClass
```

You can add more entities as needed using the same include syntax.
Prefer to include all the information into the docstring, and avoid writting Markdown text inside the markdown files of `docs` directory, except for the main title and the includes.
We plan to move to fully automatic API reference generation in the future, which would not support custom Markdown text outside the docstrings.

## Java Development Setup

You must add the Hopsworks Enterprise Edition repository to your `~/.m2/settings.xml` file in order to build the Java code.
You can get access to the repository using your nexus credentials.
Add the following to your `settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>HopsEE</id>
      <username>YOUR_NEXUS_USERNAME</username>
      <password>YOUR_NEXUS_PASSWORD</password>
    </server>
  </servers>
</settings>
```

You can then build either `hsfs` or `hsfs_utils`:

```bash
cd java
mvn clean package -Pspark-3.5,with-hops-ee
# Or
cd ../utils/python
mvn clean package
```
