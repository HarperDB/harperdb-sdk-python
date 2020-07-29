python3 -m pip install --user --upgrade setuptools wheel twine
rm -rf dist/*
python3 setup.py sdist bdist_wheel
twine upload -u FROM_LAST_PASS -p FROM_LAST_PASS dist/*
