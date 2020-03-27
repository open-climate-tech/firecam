import glob
import tensorflow as tf
from tensorflow import keras
import collect_args
import goog_helper

def _parse_function(example_proto, sequence_length=None):
    """
    Function for converting TFRecordDataset to uncompressed image pixels + labels
    :return:
    """
    feature_description = {
    'image/class/label': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'image/encoded': tf.io.FixedLenFeature([], tf.string, default_value=''),
    'image/format': tf.io.FixedLenFeature([], tf.string, default_value=''),
    'image/height': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    'image/width': tf.io.FixedLenFeature([], tf.int64, default_value=0),
    }

    # Parse the input `tf.Example` proto using the dictionary above.
    example = tf.io.parse_single_example(example_proto, feature_description)
    image = tf.image.decode_image(example['image/encoded'], channels=3)

    #Resizing images in training set because they are apprently rectangular much of the time
    if example['image/height'] != 299 or example['image/width'] != 299:
        image = tf.image.resize(tf.reshape(image, [example['image/height'], example['image/width'], 3]), [299, 299])
        image = tf.cast(image, tf.uint8)

    label = tf.one_hot(example['image/class/label'], depth=2)
    image = tf.reshape(image, [299, 299, 3]) #weird workaround because decode image doesnt get shape
    image = (tf.cast(image, tf.float32) - 128) / 128.0

    if sequence_length is None:
        #single image mode
        return [image, label]
    else:
        #sequence of images mode
        #TODO: THIS IS A PLACEHOLDER TO BE REPLACED WHEN A TRAINING SET OF IMAGE SEQUENCES IS READY
        #Right now, we just duplicate a single image several time, so the input tensor has the same
        #dimensionality as it would when real sequences are available
        image_sequence = tf.stack(sequence_length * [image], axis=0)
        return [image_sequence, label]

def build_model(type, sequence_length=None):
    """
    Construct a model. Can either take in a batch of images (i.e. N,H,W,C tensor) or a batch of time
    series (i.e. N,T,H,W,C) depending on the specific model)
    :param type: which model architecture
    :param sequence_length: how many images in sequence (needed for convolutional sequence models)
    :return:
    """
    if type == 'inception':
        #Vanilla inception model in which image is fed in and a prediction comes out
        return keras.applications.inception_v3.InceptionV3(weights=None, include_top=True, input_tensor=None, classes=2)

        keras.Model(inputs=[input_sequences], outputs=[output])
    elif type == 'inception_rnn':
        #Inception model applied to each image in a sequence, resulting sequence of vectors is passed into an LSTM
        inception = keras.applications.inception_v3.InceptionV3(weights=None, include_top=False, input_tensor=None,
                                                                pooling='max')
        input_sequences = keras.layers.Input(shape=inception.input_shape)
        #apply the same inception network to every input in the sequence
        time_series = keras.layers.TimeDistributed(inception)(input_sequences)

        rnn_state = keras.layers.LSTM(units=1024)(time_series)
        #2-class
        output = keras.layers.Dense(2, activation='softmax')(rnn_state)
        return keras.Model(inputs=[input_sequences], outputs=[output])
    elif type == 'inception_cnn':
        #Inception model applied to each image in a sequence, then results are concatenated, and passed through two more
        # convolutional layers prior to prediction
        inception = keras.applications.inception_v3.InceptionV3(weights=None, include_top=False, input_tensor=None,
                                                                pooling=None)
        input_sequences = keras.layers.Input(shape=[None, *inception.input_shape[1:]])
        # apply the same inception network to every input in the sequence
        time_series = keras.layers.TimeDistributed(inception)(input_sequences)

        activation_list = [time_series[:, index] for index in range(sequence_length)]
        #concatenate the activations from each image into a single tensor
        all_activations = keras.layers.Concatenate(axis=3)(activation_list)
        #pass through two conv layers
        convs = keras.layers.Conv2D(2048, 3)(all_activations)
        convs = keras.layers.Conv2D(2048, 3)(convs)
        pooled = keras.layers.GlobalAveragePooling2D()(convs)
        output = keras.layers.Dense(2, activation='softmax')(pooled)
        model = keras.Model(inputs=[input_sequences], outputs=[output])
        return model
    else:
        raise Exception('Unknown model architecture')


def main():
    reqArgs = [
        ["i", "inputDir", "local directory containing both smoke and nonSmoke images"],
        ["o", "outputDir", "local directory to write out TFRecords files"],
    ]
    optArgs = [
        ["t", "trainPercentage", "percentage of data to use for training vs. validation (default 90)"]
    ]

    args = collect_args.collectArgs(reqArgs, optionalArgs=optArgs, parentParsers=[goog_helper.getParentParser()])

    #how many images are used in the sequence. RNN models allow this number to vary, so this could be replaced
    #by a boolean for sequence vs not sequence.
    # sequence_length = None
    # architecture = 'inception'

    sequence_length = 3
    # architecture = 'inception_rnn'
    architecture = 'inception_cnn'

    batch_size = 32
    max_epochs = 1000
    steps_per_epoch=250
    overshoot_epochs=15 #number of epochs over which validation loss hasnt decreased to stop training at

    train_filenames = glob.glob(args.inputDir + 'firecam_train_*.tfrecord')
    val_filenames = glob.glob(args.inputDir + 'firecam_validation_*.tfrecord')


    raw_dataset_train = tf.data.TFRecordDataset(train_filenames)
    raw_dataset_val = tf.data.TFRecordDataset(val_filenames)

    parse_fn = lambda x: _parse_function(x, sequence_length)
    dataset_train = raw_dataset_train.map(parse_fn).repeat(max_epochs * steps_per_epoch).shuffle(batch_size * 5).batch(batch_size)
    dataset_val = raw_dataset_val.map(parse_fn).batch(batch_size)

    model = build_model(architecture, sequence_length if architecture == 'inception_cnn' else None)

    optimizer = tf.keras.optimizers.Adam(learning_rate=0.001, beta_1=0.9, beta_2=0.999, amsgrad=False)
    model.compile(optimizer=optimizer, loss=tf.keras.losses.BinaryCrossentropy(), metrics=['accuracy'])

    callbacks = [keras.callbacks.EarlyStopping(monitor='val_loss', patience=overshoot_epochs),
                 keras.callbacks.ModelCheckpoint(filepath=args.outputDir + 'best_model_' + architecture,
                                                 monitor='val_loss', save_best_only=True)]

    model.fit(dataset_train, validation_data=dataset_val, epochs=max_epochs, steps_per_epoch=steps_per_epoch, callbacks=callbacks)


if __name__ == "__main__":
    main()
