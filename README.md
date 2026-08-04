# Minny

**Package and project manager for MicroPython and CircuitPython**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/aivarannamaa/minny)

## What is Minny?

Minny is like [uv](https://docs.astral.sh/uv/) or [Poetry](https://python-poetry.org/docs/) for MicroPython and CircuitPython projects—it enables managing your project's development and runtime environment in a declarative way.

Minny follows a local-first deployment model: the project directory and its declarative configuration are the source of truth, while the connected device is a replaceable execution target. Think of the device filesystem as build output, not as the reliable home of your source code, credentials, configuration, or valuable device-generated data. Firmware installation or recovery, filesystem corruption, and replacing the board must not destroy the only copy of anything important.

When you run `minny deploy`, Minny reconciles the entire target filesystem with the declared project environment. It writes the declared application and package outputs and removes every other target path except those covered by no-delete rules. Deploy is therefore not an additive file-copy operation.

> **Note:** Minny's project features are intended for code kept in a local folder on your development machine. If you edit files directly on the device and treat those files as the primary copy, Minny's deployment model is not a good fit.

### Declarative

Uploading your files with [mpremote](https://docs.micropython.org/en/latest/reference/mpremote.html) or installing dependencies with [Circup](https://github.com/adafruit/circup) is straightforward, but it may become tedious if you need to reproduce the same setup on another board (or on the same board after upgrading its firmware).

With Minny, you write down your dependencies and deployment rules in [_pyproject.toml_](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/) once, and let the tool recreate the declared environment on the same board after a firmware upgrade or on another compatible board.

Here is a sample configuration for a MicroPython app:

```toml
[tool.minny.dependencies]
mip = [
    "logging",
    "github:jimmo/micropython-mlx90640",
    "-e ../pfx-330-c" # an editable local project
]

[[tool.minny.deploy.files]]
source-dir = "src"
target-dir = "/flash"
include = ["**/*.py", "configuration.json"]
compile = "auto"  # compiles .py files on the fly except boot.py, main.py, and code.py

# no need to mention deploying dependencies—these will be copied over by default
```

Each application-file rule selects paths relative to `source-dir`, which defaults to the project directory (`"."`). `include` and `exclude` default to empty lists, so application files are deployed only when selected explicitly. `target-dir = "auto"` uses the target's application root. The default `compile = "auto"` compiles Python files except the source-root entry files _boot.py_, _main.py_, and _code.py_; use an array of glob patterns for explicit compilation selection and `no-compile` for exclusions.

Here's another example for a CircuitPython app:

```toml
[tool.minny.dependencies]
pip = [
    "adafruit-circuitpython-charlcd~=2.2.0",
    "adafruit-circuitpython-logging",
    "-e ../my-shared-circuitpython-library",
]
circup = ["multi_keypad"]

[[tool.minny.deploy.files]]
include = ["*.py"]

# Packages are deployed by default, but we want to skip some transitive
# dependencies that are not required at runtime:
[[tool.minny.deploy.packages]]
exclude = ["adafruit-circuitpython-typing", "typing-extensions"] 
```

If the application uses a package from the same directory, declare it explicitly in the appropriate dependency list. For example, `pip = ["-e ."]` installs the co-located pip package and its `project.dependencies`; use the `mip` or `circup` list for packages in those ecosystems.

### Development environment

#### Local copies of dependencies

When your project has dependencies, you want your IDE and type-checker to know about them so you can get full support for code completion and type checking. Minny helps by making dependencies available locally.

Once you have your dependencies declared in _pyproject.toml_, you would run:

```bash
> minny sync
```

This makes Minny download and install all dependencies into the _.minny/lib_ subfolder of your project (just like `uv sync` creates or updates the _.venv_ folder).

Now you have a folder of .py files, which you can feed to your type-checker or IDE's language server. For example, if you're using [Basedpyright](https://github.com/DetachHead/basedpyright), you would configure it like this:

```toml
[tool.basedpyright]
...
extraPaths = [".minny/lib"]
...
```

Besides code completion and type-checking, your IDE should now allow you to Ctrl-click or Command-click from a library function call to its implementation so you can investigate the source code when the documentation is lacking.

Each successful sync writes _minny.lock_, which records the resolved packages and their files. Keep this file under version control to reproduce the selected dependency environment.

The _.minny/lib_ directory can be recreated from the project configuration and lock, so it normally does not need to be kept under version control. Minny also writes _.minny/sync-state.json_, a machine-local receipt recording the hash of the _minny.lock_ file which it last successfully materialized into the local _lib_ directory. This derived state enables fast repeated syncs and should not be committed (unless you also commit .minny/lib).

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

> **Note:** Just like with the ".minny/lib" folder, you don't need to keep the "typeshed" and "typings" folders under version control—Minny has enough information to recreate them when required.

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
    "-e ../pfx-330-c"
]
```

When you run `minny sync`, editable dependencies are represented in _lib_ by metadata and special marker files, which point to the location of the modules under their project directory.

Editable dependencies are particularly useful when:

- Developing multiple related packages simultaneously
- Contributing to open-source libraries used by your project
- Working with experimental or unreleased versions of dependencies

The application can include a co-located package by declaring `-e .` in the appropriate dependency list. Like other editable dependencies, it is then represented in _lib_ by metadata pointing to its source files.


### Runtime environment

Deploy reconciles the entire target filesystem with the declared project environment. Undeclared target paths are removed by default, so do not keep the only copy of an important file on the device. Store lasting inputs locally or elsewhere and deploy them explicitly; arrange for valuable runtime output to be exported from the device. Use no-delete rules only for target paths which deliberately remain outside reconciliation.

Once you're ready to test your code, plug in a device and execute something like this:

```bash
minny --port COM4 deploy
```

Under the hood, this command performs the following steps:

1. Perform a `minny sync` to make sure the _.minny/lib_ folder is in sync with your project specification.
2. Transfer all explicitly declared packages to your device's `lib` folder, including a co-located package declared with `-e .`. By default, Minny compiles .py files to .mpy files on the fly.
3. Copy the main files (e.g., _main.py_, _code.py_, _boot.py_, and helper modules) to the device's main folder, according to the deploy rules specified in _pyproject.toml_.
4. Remove target paths which are neither declared deployment outputs nor covered by a no-delete rule.

Now you can press Ctrl-D on your device and test your program. If you're not satisfied, edit some files and invoke the same command again—this time it will be faster as only changed files need to be updated on the device.

Alternatively, you can execute following command:

```bash
minny --port COM4 run my-test.py
```

This performs a regular `deploy`, restarts the interpreter to provide fresh VM state, and then sends the contents of _my-test.py_ to the REPL. Pass `--no-restart` to run in the current VM state instead.

Minny assumes target files are changed only through Minny and normally trusts its local record of target files and directory contents to make repeated deployment fast. Use `--rescan` with `deploy` or `run` after editing or creating device files with another tool; it rechecks desired files and refreshes target directory inventories before deployment.

Deployment rules determine the desired files and their destinations, while `tool.minny.deploy.no-delete` is the only configuration which limits exact reconciliation. Before deleting anything, Minny briefly explains whole-target reconciliation, shows the deletion candidates, and asks for confirmation; run with `-v` before the command to also see the effective deployment settings and plan counts, or use `--yes` for unattended deployment. `tool.minny.deploy.no-delete` lists target globs which pruning must retain and defaults to `["/sd", "/rom", "/ram", "/boot.py", "/boot.txt", "/flash/boot.py", "/safemode.py", "/safemode.txt", "/repl.py", "/flash/SKIPSD", "/settings.toml", "/webrepl_cfg.py", "/flash/webrepl_cfg.py", "/boot_out.txt", "/.*", "/flash/.*"]`; an explicit list replaces this default. The defaults retain conventional device-specific boot, recovery, credential, firmware-generated, and top-level hidden state, but not application entry points such as _main.py_ and _code.py_. The `--no-delete` command-line option retains all undeclared paths for one invocation. No-delete rules limit deletion only: they do not make retained data durable, and they do not prevent explicitly configured files from being created or updated at matching paths.

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

## Related work

* https://codeberg.org/yvo/mpypkg
* https://github.com/cortexm/mpytool
* https://belay.readthedocs.io/en/latest/Package%20Manager.html
