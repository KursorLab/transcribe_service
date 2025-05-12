import magic  # python-magic
from .base import BaseProcessor
from pdfminer import high_level
from docx import Document

class TextProcessor(BaseProcessor):
    @classmethod
    def can_handle(cls, mime: str, ext: str) -> bool:
        return ext in {"txt", "md", "pdf", "docx"}

    def process(self, src_path: str) -> str:
        ext = src_path.rsplit(".",1)[1].lower()
        if ext == "pdf":
            return high_level.extract_text(src_path)
        if ext == "docx":
            doc = Document(src_path)
            return "\n".join(p.text for p in doc.paragraphs)
        # md or txt
        with open(src_path, encoding="utf-8") as f:
            return f.read()