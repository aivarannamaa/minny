# Minny

**Package and project manager for MicroPython and CircuitPython**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/aivarannamaa/minny)

## What is Minny?

Minny is like [uv](https://docs.astral.sh/uv/) or [Poetry](https://python-poetry.org/docs/) for MicroPython and CircuitPython projects—it enables managing your project's development and runtime environment in a declarative way.

> **Note:** Minny's main features are useful only if you keep your project in a local folder on your development machine. You don't need it when you edit files directly on the device, but keep reading—maybe you'll start liking the local-first workflow!

### Declarative

Uploading your files with [mpremote](https://docs.micropython.org/en/latest/reference/mpremote.html) or installing dependencies with [Circup](https://github.com/adafruit/circup) is straightforward, but it may become tedious if you need to reproduce the same setup on another board (or on the same board after upgrading its firmware).

With Minny, you write down your dependencies and deployment rules in [_pyproject.toml_](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) once, and let the tool take care of setting up the device.

Here is a sample configuration for a MicroPython app:

```toml
[tool.minny.dependencies]
mip = [
    "logging",
    "github:jimmo/micropython-mlx90640",
    "-e ../pfx-330-c" # an editable local project
]

[[tool.minny.deploy.files]]
source = "src"
destination = "/flash"
include = ["**/*.py", "configuration.json"]
compile = "auto"  # compiles all .py files on the fly except main.py and boot.py

# no need to mention deploying dependencies—these will be copied over by default
```

Here's another example for a CircuitPython app:

```toml
[tool.minny.dependencies]
pip = [
    "adafruit-circuitpython-charlcd~=2.2.0",
    "adafruit-circuitpython-logging",
    "-e ../my-shared-circuitpython-library",
]
circup = ["multi_keypad"]


# [[tool.minny.deploy.files]] block not needed because of useful defaults,
# but we want to skip some transitive dependencies that are not required at runtime:
[[tool.minny.deploy.packages]]
exclude = ["adafruit-circuitpython-typing", "typing-extensions"] 
```

> **Note:** If you are creating a MicroPython or CircuitPython _package_ (not an _app_), then you may not even need `tool.minny` settings—Minny will pick up your package dependencies from `project.dependencies` or from `deps` in MicroPython's _package.json_'.

### Development environment

#### Local copies of dependencies

When your project has dependencies, you want your IDE and type-checker to know about them so you can get full support for code completion and type checking. Minny helps by making dependencies available locally.

Once you have your dependencies declared in _pyproject.toml_, you would run:

```bash
> minny sync
```

This makes Minny download and install all dependencies into the _lib_ subfolder of your project (just like `uv sync` creates or updates the _.venv_ folder).

Now you have a folder of .py files, which you can feed to your type-checker or IDE's language server. For example, if you're using [Basedpyright](https://github.com/DetachHead/basedpyright), you would configure it like this:

```toml
[tool.basedpyright]
...
extraPaths = ["lib"]
...
```

Besides code completion and type-checking, your IDE should now allow you to Ctrl-click or Command-click from a library function call to its implementation so you can investigate the source code when the documentation is lacking.

> **Note:** If you choose to keep the _lib_ folder under version control, you'll have solid dependency locking in place. If you don't need this, feel free to add the folder to .gitignore and let Minny create and populate it each time someone clones the project and starts working on it.

#### Type stubs

Type stubs are another way to support your IDE or type checker. See the documentation for more information.

Minny supports making type stubs and custom typesheds easily available for type-checkers and language servers. Here's an example _pyproject.toml_ that makes `minny sync` download and install a custom typeshed to the project's "typeshed" folder and [Pimoroni Pico MicroPython Stubs](https://pypi.org/project/pimoroni-pico-stubs/) to the "typings" folder, and configures Basedpyright to consult these folders:

```toml
...

[dependency-groups]
typeshed = ["micropython-typeshed~=0.1.*"]
typings = ["pimoroni-pico-stubs==1.21.0"]

[tool.basedpyright]
typeshedPath = "typeshed"
stubPath = "typings" # this is actually the default value, so I could have omitted this line

...
```

> **Note:** Just like with the "lib" folder, you don't need to keep the "typeshed" and "typings" folders under version control—Minny has enough information to recreate them when required.

#### More about type stubs
TODO: move this to documentation

These are _*.pyi_ files containing typing information for modules lacking embedded typing annotations (for example, modules in the MicroPython and CircuitPython standard libraries).

Type stubs for MicroPython and CircuitPython are usually published at [Python Package Index](https://pypi.org/) as pip-compatible distribution packages. See https://micropython-stubs.readthedocs.io/en/main/ and https://pypi.org/project/circuitpython-stubs/

If your type-checker or language server picks up type stubs automatically from .venv, you may want to skip Minny's support and add stub packages to the "dev" dependency group and sync your .venv with `uv sync`.


#### Editable dependencies

For local development, you often need to work with packages that are not yet published or are under active development. Minny supports editable dependencies using the same `-e` syntax as pip's requirements.txt files:

```toml
[tool.minny.dependencies]
pip = [
    "published-package>=1.0.0",
    "-e ../my-local-library",           # Relative path
    "-e /absolute/path/to/package"      # Absolute path
]

mip = [
    # You can specify a "public name" to your local package.
    # This allows overriding a transitive dependency with the same name.
    "-e github:mphacker75/pfx-330-c@../pfx-330-c"
]
```

When you run `minny sync`, editable dependencies are represented in _lib_ by metadata and special marker files, which point to the location of the modules under their project directory.

Editable dependencies are particularly useful when:

- Developing multiple related packages simultaneously
- Contributing to open-source libraries used by your project
- Working with experimental or unreleased versions of dependencies

> **Note:** The current package (in case your project represents a package (not an app) always gets installed to _lib_ in editable mode.


### Runtime environment

Once you're ready to test your code, plug in a device and execute something like this:

```bash
minny --port COM4 deploy
```

Under the hood, this command performs the following steps:

1. Perform a `minny sync` to make sure the _lib_ folder is in sync with your project specification.
2. Transfer all dependencies to your device's `lib` folder. By default, Minny compiles .py files to .mpy files on the fly.
3. If the project represents a package, build it and transfer the files to the `lib` folder, just like the dependencies.
4. Copy the main files (e.g., _main.py_, _code.py_, _boot.py_, and helper modules) to the device's main folder, according to the deploy rules specified in _pyproject.toml_.

Now you can press Ctrl-D on your device and test your program. If you're not satisfied, edit some files and invoke the same command again—this time it will be faster as only changed files need to be updated on the device.

Alternatively, you can execute following command:

```bash
minny --port COM4 run my-test.py
```

This would be like `deploy` followed by sending the contents of _my-test.py_ to the REPL, except that _main.py_ would not be updated on the board.

### Lower-level commands
If you prefer to manage your dependencies manually, you can use Minny's lower-level commands for installing, uninstalling, and listing. Some examples:

* `minny --port COM4 mip install logging`
* `minny --port /dev/ttyACM0 pip install micropython-logging`
* `minny --mount G:\lib pip install adafruit-circuitpython-ssd1306`
* `minny --dir my_project/dependencies circup install multi_keypad`
* `minny --port COM5 pip uninstall micropython-logging micropython-oled`
* `minny --port COM5 mip list --outdated`

> **Note:** Minny does not use vanilla pip, mip, or circup. See the documentation for more information.

## Minny and Thonny

Minny powers MicroPython and CircuitPython support in Thonny since version 5.0, so if you click the run button while having selected a Minny back-end, Thonny will invoke `minny run` behind the scenes.

## Project Status

**Current Version**: 0.1.0a1 (Alpha)

**Status**: Core functionality implemented, active development

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) for details.