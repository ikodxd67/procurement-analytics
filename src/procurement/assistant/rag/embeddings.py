"""Эмбеддинги: текст → вектор. Протокол, настоящая модель и заглушка для тестов.

Тот же приём, что у ``LLMClient``: индекс и поиск знают только протокол.
Настоящая модель — fastembed локально (ONNX, без torch и без сети после
первой загрузки). Заглушка ``HashEmbedder`` — для тестов и CI: тянуть гигабайт
модели ради проверки SQL незачем.
"""

from __future__ import annotations

import asyncio
import math
import re
import zlib
from collections.abc import Sequence
from typing import TYPE_CHECKING, Protocol

from procurement.config import EmbedderKind, RagSettings

if TYPE_CHECKING:
    from fastembed import TextEmbedding


class Embedder(Protocol):
    @property
    def model_name(self) -> str: ...

    @property
    def dim(self) -> int: ...

    async def embed_documents(self, texts: Sequence[str]) -> list[list[float]]: ...

    async def embed_query(self, text: str) -> list[float]: ...


class FastEmbedder:
    def __init__(self, settings: RagSettings, *, batch_size: int = 32) -> None:
        self._settings = settings
        self._batch_size = batch_size
        self._model: TextEmbedding | None = None

    @property
    def model_name(self) -> str:
        return self._settings.embedding_model

    @property
    def dim(self) -> int:
        return int(self._load().embedding_size)

    def _load(self) -> TextEmbedding:
        # Ленивая загрузка: модель — гигабайт в памяти, а импорт модуля не
        # должен её тянуть (API без поиска по документам её не использует).
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(
                model_name=self._settings.embedding_model,
                cache_dir=self._settings.model_cache_dir,
            )
        return self._model

    def _embed(self, texts: list[str]) -> list[list[float]]:
        model = self._load()
        return [vector.tolist() for vector in model.embed(texts, batch_size=self._batch_size)]

    async def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        prefixed = [self._settings.document_prefix + t for t in texts]
        # Модель считает на процессоре синхронно; в отдельном потоке, чтобы не
        # останавливать цикл событий на секунды.
        return await asyncio.to_thread(self._embed, prefixed)

    async def embed_query(self, text: str) -> list[float]:
        (vector,) = await asyncio.to_thread(self._embed, [self._settings.query_prefix + text])
        return vector


class HashEmbedder:
    """Детерминированная заглушка: «мешок основ слов», разложенный по корзинам.

    Смысла не понимает, но тексты с общими словами получают близкие векторы,
    поэтому на ней можно честно проверить поиск, ранжирование и SQL. Основа
    слова — первые пять букв: «закупок» и «закупки» попадут в одну корзину.
    """

    def __init__(self, dim: int, *, model_name: str = "hash-stems-v1") -> None:
        self._dim = dim
        self._model_name = model_name

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dim(self) -> int:
        return self._dim

    def _vector(self, text: str) -> list[float]:
        vector = [0.0] * self._dim
        for word in re.findall(r"\w+", text.lower()):
            if len(word) < 3:
                continue
            vector[zlib.crc32(word[:5].encode("utf-8")) % self._dim] += 1.0
        norm = math.sqrt(sum(v * v for v in vector)) or 1.0
        return [v / norm for v in vector]

    async def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        return [self._vector(t) for t in texts]

    async def embed_query(self, text: str) -> list[float]:
        return self._vector(text)


def build_embedder(settings: RagSettings, *, dim: int) -> Embedder:
    if settings.embedder is EmbedderKind.FASTEMBED:
        return FastEmbedder(settings)
    return HashEmbedder(dim)
