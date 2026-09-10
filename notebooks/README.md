# ECHOREPO Jupyter

JupyterLab analytical workspace for ECHOREPO.

## Data access

Notebooks access the live ECHOREPO canonical API through the internal
Docker network.

Internal API URL:

    http://echorepo-lite:8000

Use:

```python
from lib.echorepo import EchoRepo

echo = EchoRepo()

samples = echo.samples()
parameters = echo.parameters()
images = echo.images()
biodiversity = echo.biodiversity()
