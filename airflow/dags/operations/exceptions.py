class DeleteDocFromKernelException(Exception):
    ...


class DocumentToDeleteException(Exception):
    ...


class PutXMLInObjectStoreException(Exception):
    ...


class ObjectStoreError(Exception):
    ...


class RegisterUpdateDocIntoKernelException(Exception):
    ...


class LinkDocumentToDocumentsBundleException(Exception):
    def __init__(self, message, response=None, *args, **kwargs):
        self.message = message
        if response is not None:
            self.response = response
