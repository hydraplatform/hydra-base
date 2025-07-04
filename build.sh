rm -rf dist/ build/ *.egg-info     # start from a clean slate (optional but recommended)
python -m build                     # creates ./dist/*.whl and ./dist/*.tar.gz
twine upload dist/*
