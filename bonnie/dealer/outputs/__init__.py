from file_output import FileOutput
from zmq_output import ZMQOutput

__all__ = [
        'FileOutput',
        'ZMQOutput'
    ]

def list_classes():
    return [
            FileOutput,
            ZMQOutput
        ]
