
## Deploying the samples

1. Download the [Google App Engine Python SDK](https://cloud.google.com/appengine/downloads) for your platform.
2. Many samples require extra libraries to be installed. If there is a `requirements.txt`, you will need to install the dependencies with [`pip`](pip.readthedocs.org).

        pip install -t lib -r requirements.txt

3. Use `appcfg.py` to deploy the sample, you will need to specify your Project ID and a version number:

        appcfg.py update -A your-app-id -V your-version app.yaml

4. Visit `https://your-app-id.appost.com` to view your application.
