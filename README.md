# python-snippets
Useful Python/Jupyter snippets

## Install Jupyter locally (user)
```
pip3.5 install --upgrade --force-reinstall --no-cache-dir --user jupyter;
pip3.5 install --user jupyter_contrib_nbextensions;
pip3.5 install --user jupyter_nbextensions_configurator;
```

## Install in Jupyter
```
jupyter nbextensions_configurator enable --user;
jupyter contrib nbextension install --user;
```

## Install python2 kernel
```
python2 -m pip install --user --upgrade ipykernel;
python2.7 -m ipykernel install --user;
```
