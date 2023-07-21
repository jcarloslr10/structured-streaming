import os
import json
from pyspark.sql.streaming import StreamingQueryListener
from http_log_analytics_workspace import post_data


class CustomStreamingQueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        """
        As an improvement you can change os.environ to DBUtils
        and read the secrets from an Azure Key Vault.
        At the time we did this, there was no Databricks
        integration with Azure Key Vault
        """
        workspace_id = os.environ['LOG_ANALYTICS_WORKSPACE_ID']
        workspace_key = os.environ['LOG_ANALYTICS_WORKSPACE_KEY']

        body = json.dumps(event)
        log_type = 'PysparkStreamLogging'

        post_data(workspace_id, workspace_key, body, log_type)
