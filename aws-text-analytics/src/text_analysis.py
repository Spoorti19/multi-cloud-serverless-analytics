import re
from collections import Counter
from statistics import mean, median, pstdev

STOP_WORDS = set([
"i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves",
"he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs",
"themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be",
"been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or",
"because","as","until","while","of","at","by","for","with","about","against","between","into","through",
"during","before","after","above","below","to","from","up","down","in","out","on","off","over","under",
"again","further","then","once","here","there","when","where","why","how","all","any","both","each","few",
"more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t",
"can","will","just","don","should","now"
])

WORD_RE = re.compile(r"[a-zA-Z']+")
SENT_RE = re.compile(r"[^.!?]+[.!?]?", re.M)

def _tokenize(text: str):
    return WORD_RE.findall(text.lower())

def _sentences(text: str):
    raw = [s.strip() for s in SENT_RE.findall(text) if s.strip()]
    return [s for s in raw if WORD_RE.search(s)]

# A1.1 Word frequency top 20 excluding stopwords
def top_20_words(text: str):
    words = _tokenize(text)
    filtered = [w for w in words if w not in STOP_WORDS]
    return Counter(filtered).most_common(20)

# A1.2 Sentence start words top 10
def top_10_sentence_starts(text: str):
    starts = []
    for s in _sentences(text):
        tokens = _tokenize(s)
        if tokens:
            starts.append(tokens[0])
    return Counter(starts).most_common(10)

# A1.3 Sentence length distribution stats
# Sentence length = number of word tokens in each sentence
def sentence_length_stats(text: str):
    lengths = [len(_tokenize(s)) for s in _sentences(text)]
    if not lengths:
        return {"mean": 0.0, "median": 0.0, "std_dev": 0.0, "count": 0}
    return {
        "mean": float(mean(lengths)),
        "median": float(median(lengths)),
        "std_dev": float(pstdev(lengths)),
        "count": len(lengths)
    }

def run_selected_analyses(text: str, analyses: dict):
    # analyses flags: word_freq, sentence_starts, sentence_stats
    out = {}
    if analyses.get("word_freq", True):
        out["top_20_words"] = top_20_words(text)
    if analyses.get("sentence_starts", True):
        out["top_10_sentence_starts"] = top_10_sentence_starts(text)
    if analyses.get("sentence_stats", True):
        out["sentence_length_stats"] = sentence_length_stats(text)
    return out
