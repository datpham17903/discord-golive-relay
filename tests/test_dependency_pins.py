from importlib.metadata import version


def test_davey_is_the_version_native_voice_decrypts_with():
    # davey 0.2.x silently breaks DAVE media decryption for discord-native-voice 0.1.1
    assert version("davey") == "0.1.7"


def test_discord_protos_matches_the_lock():
    assert version("discord-protos") == "1.2.399"
