from analysis import run_selected_analyses

text = """Hello world. Hello again!
This is a test sentence. This is another test sentence."""
print(run_selected_analyses(text, {
    "word_freq": True,
    "sentence_starts": True,
    "sentence_stats": True
}))