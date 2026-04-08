import os
import django
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core import serializers
from tqdm import tqdm

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
django.setup()

from games.models import (
    Game, Screenshot, Keyword, Company, Series, GameEngine,
    Platform, GameCountsCache, GameCardCache, FilterSectionCache,
    Genre, Theme, PlayerPerspective, GameMode, KeywordCategory
)


def get_model_size_mb(model, count):
    """Реалистичная оценка размера модели в MB"""
    model_name = model.__name__
    if model_name == 'Screenshot':
        bytes_per_record = 195
    elif model_name == 'Game':
        bytes_per_record = 350  # Со связями больше
    elif model_name == 'Keyword':
        bytes_per_record = 180
    elif model_name == 'Company':
        bytes_per_record = 200
    elif model_name in ['Series', 'GameEngine']:
        bytes_per_record = 120
    else:
        bytes_per_record = 100
    return (count * bytes_per_record) / (1024 * 1024)


def export_model_with_progress(model, model_name, batch_size=5000):
    """Экспорт модели с индикацией прогресса"""
    total = model.objects.count()
    if total == 0:
        return model_name, []

    all_data = []

    with tqdm(total=total, desc=f"  📦 {model_name}", unit="зап", leave=False, position=0) as pbar:
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)

            if model_name == 'Game':
                batch = list(model.objects.prefetch_related(
                    'genres', 'platforms', 'keywords', 'developers', 'publishers',
                    'engines', 'themes', 'player_perspectives', 'game_modes', 'series'
                ).all()[start:end])
                serialized = json.loads(serializers.serialize('json', batch,
                                                              use_natural_foreign_keys=True,
                                                              use_natural_primary_keys=True))
                all_data.extend(serialized)
            else:
                batch = list(model.objects.all()[start:end])
                serialized = json.loads(serializers.serialize('json', batch,
                                                              use_natural_foreign_keys=True,
                                                              use_natural_primary_keys=True))
                all_data.extend(serialized)

            pbar.update(end - start)

    return model_name, all_data


# Модели для экспорта (сначала маленькие, потом большие)
MODELS = [
    ('KeywordCategory', KeywordCategory),
    ('PlayerPerspective', PlayerPerspective),
    ('GameMode', GameMode),
    ('Theme', Theme),
    ('Genre', Genre),
    ('Platform', Platform),
    ('GameEngine', GameEngine),
    ('Series', Series),
    ('Company', Company),
    ('Keyword', Keyword),
    ('GameCountsCache', GameCountsCache),
    ('GameCardCache', GameCardCache),
    ('FilterSectionCache', FilterSectionCache),
    ('Screenshot', Screenshot),
    ('Game', Game),
]

print("\n" + "=" * 70)
print("🚀 TURBO EXPORTER - Многопоточный экспорт со связями")
print("=" * 70)

print("\n🔍 Сканирование моделей...")
model_stats = []
for name, model in MODELS:
    count = model.objects.count()
    size_mb = get_model_size_mb(model, count)
    model_stats.append((name, count, size_mb))

print(f"\n📊 Найдено {len([s for s in model_stats if s[1] > 0])} моделей для экспорта:")
for name, count, size in sorted(model_stats, key=lambda x: x[1], reverse=True):
    if count > 0:
        bar_len = min(int(size / 2), 30)
        bar = "█" * bar_len + "░" * (30 - bar_len)
        print(f"   • {name:25} {count:>10,} записей  {size:>5.1f} MB  {bar}")

total_records_all = sum(count for _, count, _ in model_stats)
estimated_total_mb = sum(size for _, _, size in model_stats)
print(f"\n📈 Всего записей: {total_records_all:,}")
print(f"💾 Ожидаемый размер дампа: ~{estimated_total_mb:.1f} MB")

print(f"\n⚡ Настройки: 4 потока, пачки по 5,000 записей")
print("\n⏳ Запуск экспорта...\n")

exported_data = {}
total_exported = 0
start_time = time.time()

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(export_model_with_progress, model, name): name
               for name, model in MODELS}

    with tqdm(total=len(futures), desc="📦 Общий прогресс", unit="модель", position=1) as main_pbar:
        for future in as_completed(futures):
            name, data = future.result()
            if data:
                exported_data[name] = data
                total_exported += len(data)
            main_pbar.update(1)
            main_pbar.set_postfix({'записей': f"{total_exported:,}"})

elapsed_time = time.time() - start_time
speed = total_exported / elapsed_time if elapsed_time > 0 else 0

print(f"\n\n{'=' * 70}")
print(f"✅ ЭКСПОРТ ЗАВЕРШЕН")
print(f"{'=' * 70}")
print(f"⏱️  Время экспорта:  {elapsed_time:.1f} сек")
print(f"📊 Записей:          {total_exported:,}")
print(f"⚡ Средняя скорость: {speed:.0f} записей/сек")

print(f"\n💾 Сохранение в data.json...")
save_start = time.time()

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(exported_data, f, ensure_ascii=False, separators=(',', ':'))

save_time = time.time() - save_start
file_size = os.path.getsize('data.json') / (1024 * 1024)

print(f"\n✨ ГОТОВО!")
print(f"📁 Файл: data.json ({file_size:.2f} MB)")
print(f"💾 Ожидалось: {estimated_total_mb:.1f} MB")
print(f"📊 Отклонение: {(file_size - estimated_total_mb):+.1f} MB")
print(f"⏱️  Сохранение: {save_time:.1f} сек")
print(f"🚀 Общее время: {elapsed_time + save_time:.1f} сек")