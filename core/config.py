# core/config.py
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    index: Path = Path(__file__).parent.parent
    model_config = {
        "env_file": Path(__file__).parent.parent / ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
    }

    def resolve_path(self, relative_path: str) -> Path:
        """将相对路径转换为基于项目根目录的绝对路径"""
        parts = relative_path.split('/')
        return self.index.joinpath(*parts)


# 全局实例
settings = Settings()