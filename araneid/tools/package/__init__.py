from __future__ import print_function, absolute_import
from contextlib import contextmanager
import os
import sys
import re
import codecs
import ast
import traceback
import inspect
import subprocess
import logging
import warnings
from os.path import basename, dirname, realpath, splitext
from shutil import copyfile
from pathlib import Path
from setuptools import PackageFinder 
from zipfile import BadZipFile, ZipFile
from importlib import import_module
from araneid.core.exception import SpiderNotFound, InvalidSpider
from araneid.spider import Spider

class DependencyNotResolved(Exception):
    pass


logger = logging.getLogger(__name__)
REGEXP = [
    re.compile(r'^import (.+)$'),
    re.compile(r'^from ((?!\.+).*?) import (?:.*)$')
]

SETUP_SETTINGS_TEMPLATE = '''from setuptools import setup\n
setup(
    name='{name}',
    version='{version}',
    url='{url}',
    python_requires='{python_requires}',
    install_requires={install_requires},
    packages={packages},
    py_modules={py_modules}
)
''' 
PACAGE_TYPE=[
    'wheel',
    'egg',
    'zip',
    'tar'
]


def join(f):
    return os.path.join(os.path.dirname(__file__), f)


def filter_line(l):
    return len(l) > 0 and l[0] != "#"


@contextmanager
def _open(filename=None, mode='r'):
    """Open a file or ``sys.stdout`` depending on the provided filename.

    Args:
        filename (str): The path to the file that should be opened. If
            ``None`` or ``'-'``, ``sys.stdout`` or ``sys.stdin`` is
            returned depending on the desired mode. Defaults to ``None``.
        mode (str): The mode that should be used to open the file.

    Yields:
        A file handle.

    """
    if not filename or filename == '-':
        if not mode or 'r' in mode:
            file = sys.stdin
        elif 'w' in mode:
            file = sys.stdout
        else:
            raise ValueError('Invalid mode for file: {}'.format(mode))
    else:
        file = open(filename, mode)

    try:
        yield file
    finally:
        if file not in (sys.stdin, sys.stdout):
            file.close()


def get_all_imports(
        path, encoding=None, extra_ignore_dirs=None, follow_links=True):
    imports = set()
    raw_imports = set()
    candidates = []
    ignore_errors = False
    ignore_dirs = [".hg", ".svn", ".git", ".tox", "__pycache__", "env", "venv"]

    if extra_ignore_dirs:
        ignore_dirs_parsed = []
        for e in extra_ignore_dirs:
            ignore_dirs_parsed.append(os.path.basename(os.path.realpath(e)))
        ignore_dirs.extend(ignore_dirs_parsed)
    # appendix: mimic for file path
    if not os.path.isdir(path):
        walk = (('', [], [path]),)
    else:
        walk = os.walk(path, followlinks=follow_links)

    for root, dirs, files in walk:
        dirs[:] = [d for d in dirs if d not in ignore_dirs]

        candidates.append(os.path.basename(root))
        files = [fn for fn in files if os.path.splitext(fn)[1] == ".py"]

        candidates += [os.path.splitext(fn)[0] for fn in files]
        for file_name in files:
            file_name = os.path.join(root, file_name)
            with open(file_name, "r", encoding=encoding) as f:
                contents = f.read()
            try:
                tree = ast.parse(contents)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Import):
                        for subnode in node.names:
                            raw_imports.add(subnode.name)
                    elif isinstance(node, ast.ImportFrom):
                        raw_imports.add(node.module)
            except Exception as exc:
                if ignore_errors:
                    traceback.print_exc(exc)
                    logger.warn("Failed on file: %s" % file_name)
                    continue
                else:
                    logger.error("Failed on file: %s" % file_name)
                    raise exc

    # Clean up imports
    for name in [n for n in raw_imports if n]:
        # Sanity check: Name could have been None if the import
        # statement was as ``from . import X``
        # Cleanup: We only want to first part of the import.
        # Ex: from django.conf --> django.conf. But we only want django
        # as an import.
        cleaned_name, _, _ = name.partition('.')
        imports.add(cleaned_name)

    packages = imports - (set(candidates) & imports)
    logger.debug('Found packages: {0}'.format(packages))

    with open(join("stdlib"), "r") as f:
        data = {x.strip() for x in f}
    return list(packages - data)


def get_locally_installed_packages(encoding=None):
    packages = {}
    ignore = ["tests", "_tests", "egg", "EGG", "info"]
    for path in sys.path:
        for root, dirs, files in os.walk(path):
            for item in files:
                if "top_level" in item:
                    item = os.path.join(root, item)
                    with open(item, "r", encoding=encoding) as f:
                        package = root.split(os.sep)[-1].split("-")
                        try:
                            package_import = f.read().strip().split("\n")
                        except:  # NOQA
                            # TODO: What errors do we intend to suppress here?
                            continue
                        for i_item in package_import:
                            if ((i_item not in ignore) and
                                    (package[0] not in ignore)):
                                version = None
                                if len(package) > 1:
                                    version = package[1].replace(
                                        ".dist", "").replace(".egg", "")

                                packages[i_item] = {
                                    'version': version,
                                    'name': package[0]
                                }
    return packages


def get_import_local(imports, encoding=None):
    local = get_locally_installed_packages()
    result = []
    for item in imports:
        if item.lower() in local:
            result.append(local[item.lower()])

    # removing duplicates of package/version
    result_unique = [
        dict(t)
        for t in set([
            tuple(d.items()) for d in result
        ])
    ]

    return result_unique


def get_pkg_names(pkgs):
    """Get PyPI package names from a list of imports.

    Args:
        pkgs (List[str]): List of import names.

    Returns:
        List[str]: The corresponding PyPI package names.

    """
    result = set()
    with open(join("mapping"), "r") as f:
        data = dict(x.strip().split(":") for x in f)
    for pkg in pkgs:
        # Look up the mapped requirement. If a mapping isn't found,
        # simply use the package name.
        result.add(data.get(pkg, pkg))
    # Return a sorted list for backward compatibility.
    return sorted(result, key=lambda s: s.lower())


def get_name_without_alias(name):
    if "import " in name:
        match = REGEXP[0].match(name.strip())
        if match:
            name = match.groups(0)[0]
    return name.partition(' as ')[0].partition('.')[0].strip()


def get_pkg_deps(path):
    """Get package dependencies and their version infos which imported in a project directory or just
    a single script file.

    Args:
        path str: indicts to a project directory or jsut a single script file
    Returns:
       List[str]: The corresponding dependencies package infos
    """
    pkgs = get_all_imports(path)
    pkgs_resolved_pipy_names = get_pkg_names(pkgs)
    pkgs_resolved_local_versions = get_import_local(pkgs_resolved_pipy_names)
    return pkgs_resolved_local_versions

def get_setup_config(project_name = 'example', project_version='1.0', project_url = 'https://www.example.com', python_requires='>=python3.7', install_requires=[], packages=[], py_modules=[]):
    setup_settings = SETUP_SETTINGS_TEMPLATE.format(
        name=project_name,
        version=project_version,
        url = project_url,
        python_requires = python_requires,
        install_requires = install_requires,
        packages = packages,
        py_modules = py_modules
    )
    return setup_settings

def generate_setup_file(setup_dir, project_name='example', project_version='1.0', project_url='https://www.example.com', install_requires=[], packages=[], py_modules=[]):
    # check if setup config exists
    setup_dir = os.path.realpath(setup_dir)
    if not os.path.exists(setup_dir):
        raise FileNotFoundError('setup directory %s not found' % setup_dir)
    setup_file = os.path.join(setup_dir, 'setup.py')
    if os.path.exists(setup_file):
        logger.debug('setup.py has exists in '+ setup_dir+', please just edit it.')
        return setup_file
    setup_configs = get_setup_config(project_name=project_name, project_version=project_version, project_url=project_url, install_requires=install_requires, packages=packages, py_modules=py_modules)
    try:
        with open(setup_file, 'w+') as writer: 
            writer.write(setup_configs)
    except PermissionError:
        print('you haven\'t the permission to write setup.py in '+setup_dir+'.')
        return ''
    return setup_file

def generate_requirements_file(requirement_dir, packages=[]):
    requirement_dir = os.path.realpath(requirement_dir)
    if not os.path.exists(requirement_dir):
        raise FileNotFoundError('requirement.txt directory %s not found' % requirement_dir)
    requirement_file = os.path.join(requirement_dir, 'requirements.txt')
    if os.path.exists(requirement_file):
        logger.debug('requirements.txt has exists in '+requirement_file+', please just edit it.')
        return requirement_file
    requirement_configs = '\n'.join(packages)
    try:
        with open(requirement_file, 'w+') as writer:
             writer.write(requirement_configs)
    except PermissionError:
        print('you haven\'t the permission to write requirements.txt in '+requirement_dir+'.')
        return ''


def pip_pack_by_class(cls, package_dir=None):
    """Package a script and they dependencies by cls_name using pip standard way

    Args:
        cls_name str: indicts to a class name
    Returns:
        package path: The corresponding package path location
    """
    assert(inspect.isclass(cls))
    script_path = os.path.realpath(inspect.getfile(cls))
    script_dir = dirname(script_path)
    cls_name = cls.__name__
    # create project structure which named by cls_name.
    if not package_dir:
        project_dir = os.path.join(script_dir, cls_name)
    project_src_dir = os.path.join(project_dir, basename(project_dir))
    if not os.path.exists(project_src_dir):
        os.makedirs(project_src_dir)
    # create __init__.py
    __init__file = os.path.join(project_src_dir, '__init__.py')
    Path(__init__file).touch()
    # move script into project dir
    copyfile(script_path, os.path.join(project_src_dir, '__init__.py'))
    return pip_pack(project_dir, cls_name, package_dir)


def pip_pack(path, package_name, package_dir, package_type='wheel'):
    """Package a script and they dependencies by a script or directory using pip standard way

    Args:
        path str: indict to a class name
        package_dir str: indict to destination directory that package generated locates
    Returns:
        package path: The corresponding package path location
    """
    assert(os.path.exists(path) and str.lower(package_type) in PACAGE_TYPE)
    py_modules = ['__main__', 'setup']
    install_requires = []
    packages = []
    if package_dir:
        if not os.path.exists(package_dir):
            os.makedirs(package_dir)
   
    if not os.path.isdir(path):
        setup_dir = os.path.realpath(os.path.dirname(path))
    else:
        setup_dir = path
    # add find_packages into packages
    packages= PackageFinder.find(setup_dir)
    for dep in get_pkg_deps(path):
        name = dep.get('name')
        version = dep.get('version')
        install_requires.append(name+'=='+version)
    
    setup_file = generate_setup_file(setup_dir, package_name, packages=packages, install_requires=install_requires, py_modules=py_modules)
    requirements_file =  generate_requirements_file(setup_dir, install_requires)
    # call setup to package
    # @TODO cross platform for --home option
    if str.lower(package_type) == 'wheel':
        build_opts = 'bdist_wheel'
    elif str.lower(package_type) == 'egg':
        build_opts = 'bdist_egg'
    elif str.lower(package_type) == 'zip':
        build_opts = 'sbdist --format=zip'
    elif str.lower(package_type) == 'tar':
        build_opts = 'sbdist --format=tar'

    if not package_dir:
       package_dir = setup_dir 
    setup_run = [sys.executable, setup_file, build_opts, '--dist-dir='+package_dir]
    # into project dir before run setup
    os.chdir(setup_dir)
    setup_return = subprocess.run(setup_run, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    setup_return_code = setup_return.returncode
    setup_return_stdout = setup_return.stdout.decode('utf-8')
    # get generated  package file
    package_suffix_map = {
        'wheel':'whl',
        'egg':'egg',
        'zip': 'zip',
        'tar': 'tar.gz'
    }
    package_regex = package_dir+'/([^/]+\.{package_suffix})'.format(package_suffix=package_suffix_map[package_type])
    if setup_return_code != 0:
        logger.warning(setup_return_stdout)
        return None
    package_name = re.findall(package_regex,setup_return_stdout)
    logger.info(setup_return_stdout)
    if not package_name:
        return None
    return os.path.join(package_dir, package_name[0])
    
def get_wheel_dependencies(wheel_path):
    archive = ZipFile(wheel_path)
    requirements = []
    for f in archive.namelist():
        if not f.endswith("METADATA"):
            continue
        for l in archive.open(f).read().decode("utf-8", 'ignore').split("\n"):
            info = l.lower()
            if 'requires-dist' not in info:
                continue
            package_info = info.replace('requires-dist:','').replace(' ', '').replace('(','').replace(')','')
            requirements.append(package_info)
    return requirements

def install_wheel_dependencies(wheel_path):
    dependencies = get_wheel_dependencies(wheel_path)
    if len(dependencies) < 1:
        return
    for dep in dependencies:
        install_package(dependencies)

def install_package(package):
    install_package = [sys.executable, '-m','pip','install'] + package
    install_return = subprocess.run(install_package, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    install_return_code = install_return.returncode
    install_return_stdout = install_return.stdout.decode('utf-8')
    if install_return_code != 0:
        raise DependencyNotResolved(package)
    if install_return.stdout:
        logger.info(install_return_stdout)

def check_package(package):
    check_package = [sys.executable, '-m', 'pip', 'freeze']
    check_return = subprocess.run(check_package, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # if package specified with version, version must be checked with package name
    check_return_code = check_return.returncode
    check_return_stdout = check_return.stdout.decode('utf-8', errors='ignore')
    if check_return_code != 0:
        logger.error('pip check error')
        logger.error(check_return_stdout)
        return False
    if '=='  in package:
        installed_packages = [r for r in check_return_stdout.split()]
    else:
        installed_packages = [r.split('==')[0] for r in check_return_stdout.split()]
    if_installed =  package in installed_packages
    return  if_installed


def require(package, import_as = '', import_alias=''):
    if not package or check_package(package):
        return 
    install_package([package])

class InvalidWheel(Exception):
    
    def __init__(self, file):
        actual_file = realpath(file)
        self.message = actual_file+" is not a valid wheel file."

class InvalidWheelConventionName(Exception):

    def __init__(self, file):
        actual_file = realpath(file)
        self.message = actual_file+" is not a valid wheel name convention."

def check_wheel(wheel_file):
    ext = str.upper(splitext(wheel_file)[1])
    if ext ==  '.WHL':
        return True
    return False

def run_from_wheel(wheel_file, spider=''):
    if not check_wheel(wheel_file):
        raise InvalidWheel(wheel_file)
    sys.path.insert(0, realpath(wheel_file))
    script_name_splits = wheel_file.split('-')
    if len(script_name_splits) <= 0:
        raise InvalidWheelConventionName(wheel_file)
    # resolve pip dependencies in wheel 
    try:
        install_wheel_dependencies(wheel_file)
    except BadZipFile:
        raise InvalidWheel(wheel_file)
    except DependencyNotResolved as e:
        warnings.warn('Dependency not resolved: '+str(e))

    if spider:
        spider_name = spider
    else:
        spider_name = basename(script_name_splits[0])
    # add wheel into sys.path
    convention_spider_module_name = spider_name
    spider_module = import_module(convention_spider_module_name)
    spider = getattr(spider_module, spider_name, None)
    if not spider:
        raise SpiderNotFound(spider_name+' not found in wheel '+wheel_file)
    if not issubclass(spider, Spider):
        raise  InvalidSpider('Spider '+spider+' isn\'t a valid defined spider.')
    run_crawler(spider())

def get_spider_from_wheel(wheel_file, spider=''):
    if not check_wheel(wheel_file):
        raise InvalidWheel(wheel_file)
    sys.path.insert(0, realpath(wheel_file))
    script_name_splits = wheel_file.split('-')
    if len(script_name_splits) <= 0:
        raise InvalidWheelConventionName(wheel_file)
    # resolve pip dependencies in wheel 
    try:
        install_wheel_dependencies(wheel_file)
    except BadZipFile:
        raise InvalidWheel(wheel_file)
    except DependencyNotResolved as e:
        warnings.warn('Dependency not resolved: '+str(e))

    if spider:
        spider_name = spider
    else:
        spider_name = basename(script_name_splits[0])
    # add wheel into sys.path
    convention_spider_module_name = spider_name
    spider_module = import_module(convention_spider_module_name)
    spider = getattr(spider_module, spider_name, None)
    if not spider:
        raise SpiderNotFound(spider_name+' not found in wheel '+wheel_file)
    if not issubclass(spider, Spider):
        raise InvalidSpider('Spider '+spider+'isn\'t a valid defined spider.')
    return spider



def run_from_zip():
    pass

def run_from_py():
    pass

if __name__ == "__main__":
    print(get_pkg_deps(sys.argv[1]))
