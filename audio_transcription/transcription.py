import logging
import speech_recognition as sr


class Transcription:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def get_text(self, path):
        audio = sr.AudioData.from_file(path)
        r = sr.Recognizer()
        try:
            # for testing purposes, we're just using the default API key
            # to use another API key, use `r.recognize_google(audio, key="GOOGLE_SPEECH_RECOGNITION_API_KEY")`
            # instead of `r.recognize_google(audio)`
            self.logger.debug("Google Speech Recognition thinks you said " + r.recognize_google(audio))
            return r.recognize_google(audio)
        except sr.UnknownValueError:
            print("Google Speech Recognition could not understand audio")
        except sr.RequestError as e:
            self.logger.error("Could not request results from Google Speech Recognition service; {0}".format(e))
            