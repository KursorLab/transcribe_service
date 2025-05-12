import re, json, os
from httpx import Timeout
from deepgram import DeepgramClient, PrerecordedOptions
from dotenv import load_dotenv
from .base import BaseProcessor


load_dotenv()

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")
print(DEEPGRAM_API_KEY)
SEGMENT_DURATION = 90.0

class DeepgramProcessor(BaseProcessor):
    dg = DeepgramClient(DEEPGRAM_API_KEY)
    opts = PrerecordedOptions(
        model="nova-2", language="ru",
        diarize=True, paragraphs=True, punctuate=True
    )
    timeout = Timeout(connect=5.0, read=300.0, write=300.0, pool=300.0)

    @classmethod
    def can_handle(cls, mime: str, ext: str) -> bool:
        return mime.startswith("audio/") or mime.startswith("video/")

    def process(self, src_path: str) -> str:
        all_rows = []
        for seg_path in [src_path]:
            with open(seg_path, "rb") as f:
                resp = self.dg.listen.rest.v("1").transcribe_file(
                    {"buffer": f.read()}, self.opts, timeout=self.timeout
                )
            data = json.loads(resp.to_json())
            all_rows.extend(self._extract(data, offset=0.0))
        # simple join; you could return JSON with timestamps too
        return "\n".join(f"[{r[2]}→{r[3]}] {r[0]}: {r[1]}" for r in all_rows)

    def _extract(self, data, offset=0.0):
        rows = []
        paras = data["results"]["channels"][0]["alternatives"][0]["paragraphs"]["paragraphs"]
        for p in paras:
            speaker = f"Speaker {p['speaker']}"
            start = p["start"] + offset
            end   = p["end"]   + offset
            text  = " ".join(s["text"] for s in p.get("sentences", []))
            rows.append([speaker, text, f"{start:.2f}s", f"{end:.2f}s"])
        return rows