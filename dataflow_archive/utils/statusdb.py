import logging
import time

from ibmcloudant import CouchDbSessionAuthenticator, cloudant_v1

logger = logging.getLogger(__name__)


class StatusdbSession:
    """Wrapper class for couchdb."""

    _RETRY_ATTEMPTS = 3
    _RETRY_BACKOFF_SECONDS = 0.5  # base backoff, multiplied by attempt number

    def __init__(self, config):
        user = config.get("username")
        password = config.get("password")
        url = config.get("url")
        display_url_string = f"https://{user}:********@{url}"
        self.db_name = config.get("database")
        self.connection = cloudant_v1.CloudantV1(
            authenticator=CouchDbSessionAuthenticator(user, password)
        )
        self.connection.set_service_url(f"https://{url}")
        try:
            self._retry_call(
                lambda: self.connection.get_server_information().get_result()
            )
        except Exception as e:
            raise Exception(
                f"Couchdb connection failed for URL {display_url_string} with error: {e}"
            )

    def _retry_call(self, func):
        """Call func() and retry transient failures with backoff.

        func should be a zero-arg callable that executes the cloudant SDK call
        and returns the .get_result() value (or raises).
        """
        attempts = self._RETRY_ATTEMPTS
        backoff = self._RETRY_BACKOFF_SECONDS
        last_exception = None
        for attempt in range(1, attempts + 1):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt >= attempts:
                    logger.error(f"Operation failed after {attempt} attempts: {e}")
                    break
                logger.warning(
                    f"An error occurred on attempt {attempt}/{attempts}: {e} — retrying after {backoff * (attempt)}s"
                )
                time.sleep(backoff * attempt)
        # re-raise last exception for caller to handle
        raise last_exception

    def get_db_doc(self, ddoc, view, run_id):
        """Retrieve a document from the database via retried call."""
        doc_id = self.get_doc_id(ddoc, view, run_id)
        if doc_id:
            return self._retry_call(
                lambda: self.connection.get_document(
                    db=self.db_name, doc_id=doc_id
                ).get_result()
            )
        return None

    def get_doc_id(self, ddoc, view, run_id):
        """Retrieve a document ID from the database via retried call."""
        result = self._retry_call(
            lambda: self.connection.post_view(
                db=self.db_name,
                ddoc=ddoc,
                view=view,
                key=run_id,
            ).get_result()
        )
        if result and "rows" in result and len(result["rows"]) > 0:
            return result["rows"][0]["id"]
        else:
            return None

    def get_events(self, run_id):
        """Retrieve events for a run from the database via retried call."""
        return self._retry_call(
            lambda: self.connection.post_view(
                db=self.db_name,
                ddoc="events",
                view="current_status_per_runfolder",
                key=run_id,
            ).get_result()
        )

    def update_db_doc(self, db_doc):
        """Upload document to the database via retried call."""
        try:
            self._retry_call(
                lambda: self.connection.post_document(
                    db=self.db_name, document=db_doc
                ).get_result()
            )
        except Exception as e:
            logger.error(
                "Failed to update document %s after retries: %s",
                db_doc.get("runfolder_id", "unknown"),
                e,
            )
            raise
