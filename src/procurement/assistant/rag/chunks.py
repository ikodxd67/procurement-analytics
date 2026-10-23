"""Кусок документа и нарезка длинного текста."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

# Предел длины куска в символах. У модели по умолчанию вход 512 токенов, всё
# дальше она молча отрезает — и хвост пункта в поиск не попадёт. Сколько
# токенов выходит на русском тексте правил, замерено токенизатором модели на
# настоящих кусках (см. devlog, этап 3).
MAX_CHUNK_CHARS = 1200


@dataclass(frozen=True, slots=True)
class DocChunk:
    source: str
    """Источник: ``adilet:V2400035238``, ``project:data_dictionary``."""

    anchor: str
    """Место внутри источника, уникальное в нём: id абзаца на adilet, раздел описания."""

    url: str
    title: str
    """Путь к куску: «Глава 8 › Параграф 3 › п. 112». Идёт и в поиск, и в ответ."""

    text: str

    @property
    def embed_text(self) -> str:
        # Заголовок в тексте эмбеддинга: пункт «113. Секретарь комиссии…» сам по
        # себе не говорит, что речь о конкурсе. Путь «Глава 8. … конкурса» —
        # говорит.
        return f"{self.title}\n{self.text}"

    def content_hash(self, model: str) -> str:
        # Модель входит в хэш: тот же текст другой моделью — другой вектор.
        payload = "\x1f".join((model, self.url, self.title, self.text))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def split_long(text: str, limit: int = MAX_CHUNK_CHARS) -> list[str]:
    """Разрезать текст по строкам, а строку длиннее предела — по предложениям.

    Режем по границам смысла, а не посреди слова: кусок с обрывком фразы хуже
    находится и хуже читается в ответе.
    """
    if len(text) <= limit:
        return [text]

    pieces: list[str] = []
    for line in text.split("\n"):
        if len(line) <= limit:
            pieces.append(line)
            continue
        # Граница предложения — точка, «;» или «:» после буквы, не после цифры:
        # «55. Требование…» и «пунктами 1 - 6» резать нельзя.
        sentences = re.split(r"(?<=[^\d\s][.;:])\s+", line)
        buf = ""
        for sentence in sentences:
            if len(sentence) > limit:
                # Предложение длиннее предела — крайний случай. Сначала сбросить
                # накопленное, иначе начало строки окажется после её середины
                # (так «55.» уезжал во вторую часть пункта — поймано на правилах).
                if buf:
                    pieces.append(buf)
                    buf = ""
                while len(sentence) > limit:
                    pieces.append(sentence[:limit])
                    sentence = sentence[limit:]
            if buf and len(buf) + 1 + len(sentence) > limit:
                pieces.append(buf)
                buf = sentence
            else:
                buf = f"{buf} {sentence}".strip()
        if buf:
            pieces.append(buf)

    parts: list[str] = []
    current = ""
    for piece in pieces:
        if current and len(current) + 1 + len(piece) > limit:
            parts.append(current)
            current = piece
        else:
            current = f"{current}\n{piece}" if current else piece
    if current:
        parts.append(current)
    return parts
