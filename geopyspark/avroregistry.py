custom_decoders = {}
custom_encoders = {}

def add_decoder(custom_cls, decoding_method):
    custom_decoders[custom_cls.__name__] = decoding_method

def add_encoder(custom_cls, encoding_method):
    custom_encoders[custom_cls.__name__] = encoding_method
