import os
import json
import django
from django.core import serializers
from tqdm import tqdm

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
django.setup()

from games.models import (
    Game, Screenshot, Keyword, Company, Series, GameEngine,
    Platform, GameCountsCache, GameCardCache, FilterSectionCache,
    Genre, Theme, PlayerPerspective, GameMode, KeywordCategory
)

MODELS_MAP = {
    'KeywordCategory': KeywordCategory,
    'PlayerPerspective': PlayerPerspective,
    'GameMode': GameMode,
    'Theme': Theme,
    'Genre': Genre,
    'Platform': Platform,
    'GameEngine': GameEngine,
    'Series': Series,
    'Company': Company,
    'Keyword': Keyword,
    'GameCountsCache': GameCountsCache,
    'GameCardCache': GameCardCache,
    'FilterSectionCache': FilterSectionCache,
    'Screenshot': Screenshot,
    'Game': Game,
}


def import_data():
    print("\n" + "=" * 70)
    print("🚀 ИМПОРТ ДАННЫХ ИЗ data.json")
    print("=" * 70)

    with open('data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"\n📁 Загружено {len(data)} моделей из data.json")

    total_objects = 0
    for model_name, objects_data in tqdm(data.items(), desc="Импорт моделей", unit="модель"):
        if model_name in MODELS_MAP:
            model = MODELS_MAP[model_name]
            # Конвертируем JSON обратно в объекты Django
            deserialized_objects = []
            for obj_data in objects_data:
                # Для Game нужно сохранять связи
                deserialized = serializers.deserialize('json', json.dumps([obj_data]))
                for obj in deserialized:
                    deserialized_objects.append(obj)

            # Сохраняем объекты
            for obj in tqdm(deserialized_objects, desc=f"  📦 {model_name}", leave=False):
                obj.save()
                total_objects += 1

    print(f"\n✨ ИМПОРТ ЗАВЕРШЕН!")
    print(f"📊 Импортировано объектов: {total_objects}")


if __name__ == '__main__':
    import_data()