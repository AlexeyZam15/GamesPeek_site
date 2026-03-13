"""Views for game analysis."""

from django.utils.safestring import mark_safe
import json
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, JsonResponse
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.utils import timezone
from django.urls import reverse

from typing import Dict, Any, List, Tuple, Optional
from ..models import Game, Keyword, KeywordCategory
from ..analyze.game_analyzer_api import GameAnalyzerAPI
from django.db import models
import html
import re
from games.management.commands.normalize_keywords import Command as NormalizeCommand


# ===== УТИЛИТЫ ДЛЯ ПРОВЕРКИ ПРАВ =====
def is_staff_or_superuser(user):
    """Проверяет, является ли пользователь staff или superuser"""
    return user.is_authenticated and (user.is_staff or user.is_superuser)


# ===== НОВЫЕ AJAX ФУНКЦИИ =====

@login_required
@user_passes_test(is_staff_or_superuser)
def analyze_game_ajax(request: HttpRequest, game_id: int):
    """
    AJAX обработчик для анализа текста игры без перезагрузки страницы.
    Возвращает JSON с подсвеченным HTML и информацией о найденных элементах.
    """
    if request.method != 'POST':
        return JsonResponse({'success': False, 'error': 'Invalid request method'}, status=405)

    try:
        data = json.loads(request.body)
        tab_key = data.get('tab', 'summary')

        game = get_object_or_404(Game, pk=game_id)

        # Получаем оригинальный текст для указанной вкладки
        text_sources = {
            'summary': game.summary,
            'storyline': game.storyline,
            'rawg': game.rawg_description,
            'wiki': game.wiki_description,
        }
        original_text = text_sources.get(tab_key, '')

        if not original_text:
            return JsonResponse({
                'success': False,
                'error': f'No text found for the "{tab_key}" tab.'
            })

        print(f"\n=== АНАЛИЗ ТЕКСТА для игры {game_id}, вкладка {tab_key} ===")
        print(f"Длина текста: {len(original_text)}")
        print(f"Первые 200 символов: {original_text[:200]}")

        # Очищаем кэш Trie перед анализом
        try:
            from games.analyze.keyword_trie import KeywordTrieManager
            KeywordTrieManager().clear_cache()
            print("✅ Кэш Trie очищен")
        except Exception as e:
            print(f"⚠️ Ошибка при очистке кэша Trie: {e}")

        # Выполняем анализ текста
        analyzer = GameAnalyzerAPI(verbose=True)

        analysis_result = analyzer.analyze_game_text_comprehensive(
            text=original_text,
            game_id=game.id,
            existing_game=game,
            exclude_existing=False  # Находим все, включая существующие
        )

        print(f"\n=== РЕЗУЛЬТАТЫ АНАЛИЗА ===")
        print(f"Успех: {analysis_result['success']}")
        print(f"Есть результаты: {analysis_result.get('has_results', False)}")
        print(f"Найдено элементов: {analysis_result['summary'].get('found_count', 0)}")
        print(f"Всего совпадений: {analysis_result.get('total_matches', 0)}")

        # Детальный вывод результатов
        if 'results' in analysis_result:
            for category, data in analysis_result['results'].items():
                if data and 'items' in data:
                    print(f"  {category}: {len(data['items'])} элементов")
                    for item in data['items']:
                        print(f"    - {item.get('name', 'Unknown')}")

        if not analysis_result['success']:
            return JsonResponse({
                'success': False,
                'error': analysis_result.get('error', 'Unknown analysis error')
            })

        # Форматируем текст в HTML с сохранением абзацев
        formatted_text = format_text_with_html(original_text)

        # Применяем подсветку используя pattern_info из результата анализа
        # Передаем и оригинальный текст для поиска позиций
        highlighted_text = apply_highlights_from_pattern_info(
            formatted_text,
            original_text,
            analysis_result.get('pattern_info', {})
        )

        # Извлекаем найденные элементы с правильным флагом is_new
        found_items = extract_found_items_combined(analysis_result, game)

        # Подготавливаем данные для сохранения
        save_data = prepare_save_data(analysis_result, game, tab_key)

        # Сохраняем результаты в сессию
        session_key = f'unsaved_results_{game_id}'
        unsaved_results = request.session.get(session_key, {
            'highlighted_text': {},
            'found_items': {},
            'analysis_data': {},
            'save_data': {}
        })

        unsaved_results['highlighted_text'][tab_key] = highlighted_text
        unsaved_results['found_items'][tab_key] = found_items
        unsaved_results['analysis_data'][tab_key] = {
            'text_source': tab_key,
            'results': analysis_result.get('results', {}),
            'summary': analysis_result.get('summary', {}),
            'pattern_info': analysis_result.get('pattern_info', {}),
            'original_text': original_text,
            'formatted_text': formatted_text
        }
        unsaved_results['save_data'] = save_data

        request.session[session_key] = unsaved_results
        request.session.modified = True

        # Формируем успешный JSON ответ
        total_found = analysis_result['summary'].get('found_count', 0)
        total_matches = analysis_result.get('total_matches', 0)

        response_data = {
            'success': True,
            'highlighted_html': highlighted_text,
            'found_items': found_items,
            'summary': {
                'found_count': total_found,
                'total_matches': total_matches,
                'has_results': total_found > 0,
            },
            'message': f'Found {total_found} elements with {total_matches} matches.',
            'has_unsaved_results': True
        }

        print(f"✅ Отправляем ответ с {total_found} элементами")
        return JsonResponse(response_data)

    except Game.DoesNotExist:
        print(f"❌ Игра {game_id} не найдена")
        return JsonResponse({'success': False, 'error': 'Game not found.'})
    except Exception as e:
        print(f"❌ ОШИБКА в analyze_game_ajax: {str(e)}")
        import traceback
        traceback.print_exc()
        return JsonResponse({'success': False, 'error': f'Server error: {str(e)}'})

@login_required
@user_passes_test(is_staff_or_superuser)
def add_keyword_ajax(request: HttpRequest, game_id: int):
    """AJAX добавление ключевого слова без перезагрузки страницы"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

    try:
        import json
        data = json.loads(request.body)
        keyword_name = data.get('keyword', '').strip()

        if not keyword_name:
            return JsonResponse({'success': False, 'message': 'Please enter a keyword'})

        game = get_object_or_404(Game, pk=game_id)

        # Создаем или получаем ключевое слово
        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()
        was_created = False

        if not keyword:
            other_category, created = KeywordCategory.objects.get_or_create(
                name='Other',
                defaults={'description': 'Manually added keywords'}
            )

            max_igdb_id = Keyword.objects.aggregate(models.Max('igdb_id'))['igdb_id__max'] or 1000000
            new_igdb_id = max_igdb_id + 1

            keyword = Keyword.objects.create(
                name=keyword_name,
                category=other_category,
                igdb_id=new_igdb_id,
                cached_usage_count=1
            )
            was_created = True
            print(f"✅ Ключевое слово '{keyword_name}' создано (Trie будет обновлен при следующем анализе)")

        # Добавляем ключевое слово к игре (если его еще нет)
        already_exists = False
        if keyword in game.keywords.all():
            already_exists = True
        else:
            game.keywords.add(keyword)
            # Обновляем кэш
            keyword.update_cached_count(force=True)
            game.update_cached_counts(force=True)

        return JsonResponse({
            'success': True,
            'message': f'Keyword "{keyword_name}" added to game!',
            'keyword': {
                'id': keyword.id,
                'name': keyword.name
            },
            'was_created': was_created,
            'already_exists': already_exists
        })

    except Exception as e:
        print(f"✗ ОШИБКА добавления ключевого слова: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': f'Error adding keyword: {str(e)}'
        })


@login_required
@user_passes_test(is_staff_or_superuser)
def delete_keyword_ajax(request: HttpRequest, game_id: int):
    """AJAX удаление ключевого слова без перезагрузки страницы"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

    try:
        import json
        data = json.loads(request.body)
        keyword_name = data.get('keyword', '').strip()

        if not keyword_name:
            return JsonResponse({'success': False, 'message': 'Please enter a keyword'})

        game = get_object_or_404(Game, pk=game_id)
        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()

        if not keyword:
            return JsonResponse({
                'success': False,
                'message': f'Keyword "{keyword_name}" not found in database'
            })

        # Проверяем, есть ли ключевое слово у этой игры
        if keyword not in game.keywords.all():
            return JsonResponse({
                'success': False,
                'message': f'Keyword "{keyword_name}" is not associated with this game'
            })

        # Удаляем связь с игрой
        game.keywords.remove(keyword)

        # Обновляем кэш игры
        game.update_cached_counts(force=True)

        # Получаем популярность (сколько игр используют это ключевое слово)
        popularity = keyword.game_set.count()

        return JsonResponse({
            'success': True,
            'message': f'Keyword "{keyword_name}" removed from game',
            'keyword': {
                'id': keyword.id,
                'name': keyword.name
            },
            'popularity': popularity,
            'still_exists_in_db': popularity > 0
        })

    except Exception as e:
        print(f"✗ ОШИБКА удаления ключевого слова: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': f'Error deleting keyword: {str(e)}'
        })

@login_required
@user_passes_test(is_staff_or_superuser)
def analyze_single_game(request: HttpRequest, game_id: int):
    """Анализ одной игры - ТОЛЬКО комбинированный режим"""

    # Проверяем, нужно ли выполнить автоматический анализ (удаляем эту логику - не нужна)
    # Весь блок с auto_analyze_key удаляем

    # ДОБАВЛЯЕМ: Обновляем Trie перед каждым GET запросом (для актуальности данных)
    try:
        from games.analyze.keyword_trie import KeywordTrieManager
        KeywordTrieManager().clear_cache()
        print(f"✅ Кэш Trie обновлен при загрузке страницы для игры {game_id}")
    except Exception as e:
        print(f"⚠️ Ошибка при обновлении Trie: {e}")

    # Инициализация
    game, original_descriptions, active_tab = _initialize_analysis_context(request, game_id)

    # Обработка POST запросов
    if request.method == 'POST':
        return _handle_post_request(request, game, original_descriptions, active_tab)

    # Подготовка контекста для GET запроса
    context = _prepare_get_context(request, game, original_descriptions, active_tab)

    return render(request, 'games/analyze.html', context)


def remove_existing_highlights(html_text: str) -> str:
    """
    Удаляет существующие span теги подсветки из HTML текста
    """
    import re

    def remove_span(match):
        # Возвращаем только текст внутри span
        return match.group(1)

    # Удаляем все span с классами highlight-
    pattern = r'<span[^>]*class="[^"]*highlight-[^"]*"[^>]*>(.*?)</span>'

    # Продолжаем заменять, пока есть совпадения
    prev_text = None
    current_text = html_text

    while prev_text != current_text:
        prev_text = current_text
        current_text = re.sub(pattern, remove_span, current_text, flags=re.DOTALL | re.IGNORECASE)

    return current_text


def find_html_position(html_text: str, clean_text: str, clean_pos: int) -> int:
    """
    Находит позицию в HTML тексте, соответствующую позиции в чистом тексте
    """
    html_idx = 0
    clean_idx = 0
    in_tag = False

    while html_idx < len(html_text) and clean_idx <= clean_pos:
        char = html_text[html_idx]

        if char == '<':
            in_tag = True
            html_idx += 1
            continue
        elif char == '>':
            in_tag = False
            html_idx += 1
            continue

        if not in_tag:
            if clean_idx == clean_pos:
                return html_idx
            clean_idx += 1

        html_idx += 1

    return -1


def is_inside_html_tag(html_text: str, position: int) -> bool:
    """
    Проверяет, находится ли позиция внутри HTML тега
    """
    if position < 0 or position >= len(html_text):
        return False

    # Находим последний '<' до этой позиции
    last_lt = html_text.rfind('<', 0, position)
    if last_lt == -1:
        return False

    # Находим следующий '>' после этого '<'
    next_gt = html_text.find('>', last_lt)
    if next_gt == -1:
        return True  # Незакрытый тег

    # Если позиция между '<' и '>', значит внутри тега
    return last_lt < position < next_gt


def apply_highlights_from_pattern_info(html_text: str, original_text: str, pattern_info: Dict) -> str:
    """
    Применяет подсветку к HTML тексту, используя данные из pattern_info.
    Исправлено: предотвращение вложенных подсветок.
    """
    if not html_text or not pattern_info or not original_text:
        return html_text

    try:
        # Удаляем старую подсветку
        html_text = remove_existing_highlights(html_text)

        # Получаем чистый текст из HTML для поиска
        import re
        clean_text = re.sub(r'<[^>]+>', '', html_text)

        if not clean_text:
            return html_text

        # Собираем все совпадения для подсветки
        categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
        category_classes = {
            'genres': 'highlight-genre',
            'themes': 'highlight-theme',
            'perspectives': 'highlight-perspective',
            'game_modes': 'highlight-game_mode',
            'keywords': 'highlight-keyword'
        }

        # Словарь для хранения всех позиций для подсветки
        highlight_positions = []

        for category in categories:
            for match in pattern_info.get(category, []):
                if match.get('status') == 'found':
                    matched_text = match.get('matched_text', '')

                    if not matched_text:
                        continue

                    # Ищем все вхождения этого текста в чистом тексте
                    search_text = matched_text.lower()
                    clean_lower = clean_text.lower()

                    pos = 0
                    while True:
                        found_pos = clean_lower.find(search_text, pos)
                        if found_pos == -1:
                            break

                        # Проверяем границы слова
                        is_valid = True

                        # Проверяем начало
                        if found_pos > 0:
                            prev_char = clean_text[found_pos - 1]
                            if prev_char.isalnum() and prev_char != '-':
                                is_valid = False

                        # Проверяем конец
                        end_pos = found_pos + len(matched_text)
                        if end_pos < len(clean_text):
                            next_char = clean_text[end_pos]
                            if next_char.isalnum() and next_char not in "s'-":
                                is_valid = False

                        if is_valid:
                            # Находим соответствующую позицию в HTML
                            html_start = find_html_position(html_text, clean_text, found_pos)
                            if html_start != -1:
                                html_end = html_start + len(matched_text)

                                # Проверяем, что не внутри HTML тега
                                if not is_inside_html_tag(html_text, html_start):
                                    highlight_positions.append({
                                        'start': html_start,
                                        'end': html_end,
                                        'name': match['name'],
                                        'category': category,
                                        'class_name': category_classes[category],
                                        'matched_text': matched_text,
                                        'length': len(matched_text)
                                    })

                        pos = found_pos + 1

        if not highlight_positions:
            return html_text

        # Удаляем дубликаты (одинаковые позиции)
        unique_positions = []
        seen = set()
        for pos in highlight_positions:
            key = (pos['start'], pos['end'])
            if key not in seen:
                seen.add(key)
                unique_positions.append(pos)

        # Находим и разрешаем конфликты (пересекающиеся диапазоны)
        # Сортируем по длине (сначала самые длинные)
        unique_positions.sort(key=lambda x: x['length'], reverse=True)

        # Отмечаем, какие позиции уже заняты
        occupied = []
        final_positions = []

        for pos in unique_positions:
            # Проверяем, не пересекается ли с уже занятыми
            overlaps = False
            for occ_start, occ_end in occupied:
                if not (pos['end'] <= occ_start or pos['start'] >= occ_end):
                    overlaps = True
                    break

            if not overlaps:
                final_positions.append(pos)
                occupied.append((pos['start'], pos['end']))

        # Сортируем финальные позиции от конца к началу
        final_positions.sort(key=lambda x: x['start'], reverse=True)

        # Применяем подсветку
        highlighted_text = html_text
        for pos in final_positions:
            try:
                # Проверяем границы
                if pos['start'] < 0 or pos['end'] > len(highlighted_text):
                    continue
                if pos['start'] >= pos['end']:
                    continue

                # Получаем текст для подсветки
                text_to_highlight = highlighted_text[pos['start']:pos['end']]

                # Пропускаем, если текст уже содержит span
                if '<span' in text_to_highlight and '</span>' in text_to_highlight:
                    continue

                # Создаем подсвеченный span
                category_display = {
                    'genres': 'Genre',
                    'themes': 'Theme',
                    'perspectives': 'Perspective',
                    'game_modes': 'Game Mode',
                    'keywords': 'Keyword'
                }.get(pos['category'], pos['category'])

                # Экранируем текст
                safe_text = html.escape(text_to_highlight)

                highlighted_span = (
                    f'<span class="{pos["class_name"]}" '
                    f'data-element-name="{html.escape(pos["name"])}" '
                    f'data-category="{pos["category"]}" '
                    f'title="{category_display}: {html.escape(pos["name"])}">'
                    f'{safe_text}'
                    f'</span>'
                )

                # Заменяем в тексте
                highlighted_text = (
                        highlighted_text[:pos['start']] +
                        highlighted_span +
                        highlighted_text[pos['end']:]
                )

            except Exception as e:
                print(f"Ошибка при подсветке: {e}")
                continue

        return highlighted_text

    except Exception as e:
        print(f"❌ Ошибка в apply_highlights_from_pattern_info: {e}")
        import traceback
        traceback.print_exc()
        return html_text


def extract_found_items_combined(analysis_result: Dict, game=None) -> Dict:
    """
    Извлекает найденные элементы с правильным флагом is_new
    """
    results = {}
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

    for category in categories:
        items = analysis_result.get('results', {}).get(category, {}).get('items', [])
        if items:
            found_items = []
            new_count = 0
            seen_ids = set()

            for item in items:
                # Пропускаем дубликаты по ID
                if item['id'] in seen_ids:
                    continue
                seen_ids.add(item['id'])

                # Проверяем, есть ли уже у игры
                is_new = True
                if game:
                    if category == 'keywords':
                        is_new = not game.keywords.filter(id=item['id']).exists()
                    elif category == 'genres':
                        is_new = not game.genres.filter(id=item['id']).exists()
                    elif category == 'themes':
                        is_new = not game.themes.filter(id=item['id']).exists()
                    elif category == 'perspectives':
                        is_new = not game.player_perspectives.filter(id=item['id']).exists()
                    elif category == 'game_modes':
                        is_new = not game.game_modes.filter(id=item['id']).exists()

                if is_new:
                    new_count += 1

                found_items.append({
                    'name': item['name'],
                    'id': item['id'],
                    'is_new': is_new
                })

            results[category] = found_items
            results[f'{category}_new_count'] = new_count
        else:
            results[f'{category}_new_count'] = 0

    # Общая статистика
    total_found = 0
    for category in categories:
        total_found += len(results.get(category, []))
    results['total_found'] = total_found

    return results


def prepare_save_data(analysis_result: Dict, game: Game, tab: str) -> Dict:
    """
    Подготавливает данные для сохранения - ТОЛЬКО НОВЫЕ элементы
    """
    save_data = {
        'text_source': tab,
        'results': {},
        'found_count': 0
    }

    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    total_new = 0

    for category in categories:
        items = analysis_result.get('results', {}).get(category, {}).get('items', [])
        if not items:
            continue

        # Получаем существующие ID игры
        if category == 'keywords':
            existing_ids = set(game.keywords.values_list('id', flat=True))
        elif category == 'genres':
            existing_ids = set(game.genres.values_list('id', flat=True))
        elif category == 'themes':
            existing_ids = set(game.themes.values_list('id', flat=True))
        elif category == 'perspectives':
            existing_ids = set(game.player_perspectives.values_list('id', flat=True))
        elif category == 'game_modes':
            existing_ids = set(game.game_modes.values_list('id', flat=True))
        else:
            existing_ids = set()

        # Фильтруем только новые элементы
        category_new_items = []
        seen_ids = set()

        for item in items:
            # Проверяем по ID
            if item['id'] not in existing_ids and item['id'] not in seen_ids:
                seen_ids.add(item['id'])
                category_new_items.append(item)

        if category_new_items:
            save_data['results'][category] = {'items': category_new_items}
            total_new += len(category_new_items)

    save_data['found_count'] = total_new
    return save_data

def _handle_post_request(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка POST запросов"""

    # Добавление ключевого слова (старый метод - оставляем для обратной совместимости)
    if 'add_keyword' in request.POST:
        return _handle_add_keyword(request, game, original_descriptions, active_tab)

    # Анализ текста
    elif 'analyze' in request.POST:
        # ДОБАВЛЯЕМ: Принудительное обновление Trie перед анализом
        try:
            from games.analyze.keyword_trie import KeywordTrieManager
            KeywordTrieManager().clear_cache()
            print(f"✅ Кэш Trie обновлен перед анализом для игры {game.id}")
        except Exception as e:
            print(f"⚠️ Ошибка при обновлении Trie перед анализом: {e}")

        return _handle_analyze_request(request, game, original_descriptions, active_tab)

    # Сохранение результатов
    elif 'save_results' in request.POST:
        return _handle_save_results(request, game, original_descriptions, active_tab)

    # По умолчанию - редирект на ту же вкладку
    return _redirect_to_tab(game.id, active_tab)


def _handle_add_keyword(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка добавления ключевого слова (старый метод - только для обратной совместимости)"""
    keyword_name = request.POST.get('new_keyword', '').strip()
    analyze_tab = request.POST.get('analyze_tab', active_tab)

    if not keyword_name:
        messages.error(request, '❌ Please enter a keyword')
        return _redirect_to_tab(game.id, analyze_tab)

    try:
        # Создаем или получаем ключевое слово
        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()

        if not keyword:
            other_category, created = KeywordCategory.objects.get_or_create(
                name='Other',
                defaults={'description': 'Manually added keywords'}
            )

            max_igdb_id = Keyword.objects.aggregate(models.Max('igdb_id'))['igdb_id__max'] or 1000000
            new_igdb_id = max_igdb_id + 1

            keyword = Keyword.objects.create(
                name=keyword_name,
                category=other_category,
                igdb_id=new_igdb_id,
                cached_usage_count=1
            )
            messages.success(request, f'✅ Created new keyword: "{keyword_name}"')
        else:
            messages.info(request, f'ℹ️ Keyword "{keyword_name}" already exists')

        # Добавляем ключевое слово к игре
        game.keywords.add(keyword)

        # Обновляем кэш
        keyword.update_cached_count(force=True)
        game.update_cached_counts(force=True)

        messages.success(request, f'✅ Keyword "{keyword_name}" added to game!')

        # УБИРАЕМ весь автоанализ - не нужно

        redirect_url = reverse('analyze_game', args=[game.id])
        params = []
        if analyze_tab and analyze_tab != 'summary':
            params.append(f'tab={analyze_tab}')
        params.append('keyword_added=1')
        if params:
            redirect_url += '?' + '&'.join(params)

        return redirect(redirect_url)

    except Exception as e:
        messages.error(request, f'❌ Error adding keyword: {str(e)}')
        return _redirect_to_tab(game.id, analyze_tab)


def _handle_analyze_request(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка запроса на анализ"""
    analyze_tab = request.POST.get('analyze_tab', active_tab)

    # Обновляем флаг подсветки
    highlight_toggle = request.POST.get('highlight_toggle')
    if highlight_toggle is not None:
        highlight_enabled = (highlight_toggle == 'on')
        request.session[f'highlight_enabled_{game.id}'] = highlight_enabled

    # Проверяем наличие текста для анализа
    if analyze_tab not in original_descriptions:
        messages.error(request, '❌ Selected tab has no text')
        return _redirect_to_tab(game.id, active_tab)

    text = original_descriptions[analyze_tab]

    if not text:
        messages.error(request, '❌ No text to analyze.')
        return _redirect_to_tab(game.id, active_tab)

    try:
        analyzer = GameAnalyzerAPI(verbose=True)

        # Выполняем анализ оригинального текста
        analysis_result = analyzer.analyze_game_text_comprehensive(
            text=text,
            game_id=game.id,
            existing_game=game,
            exclude_existing=False
        )

        if analysis_result['success']:
            # Сохраняем результаты анализа
            _save_analysis_results(request, game.id, analyze_tab, text, analysis_result)

            # Сообщение о результатах
            total_found = analysis_result['summary'].get('found_count', 0)
            total_matches = analysis_result.get('total_matches', 0)

            if total_found > 0:
                messages.success(request,
                                 f'🔍 Found {total_found} elements with {total_matches} total matches. '
                                 f'All matches highlighted. Click "Save Results" to save new elements to database.')
            else:
                messages.info(request,
                              'ℹ️ No new elements found in text. Existing elements are highlighted.')

            # Обновляем активную вкладку
            active_tab = analyze_tab
        else:
            messages.error(request, f'❌ Analysis error: {analysis_result.get("error", "Unknown error")}')

    except Exception as e:
        messages.error(request, f'❌ Error during analysis: {str(e)}')

    return _redirect_to_tab(game.id, active_tab)


def delete_keyword(request: HttpRequest, game_id: int):
    """Обработка удаления ключевого слова (старый метод - только для обратной совместимости)"""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

    try:
        import json
        data = json.loads(request.body)
        keyword_name = data.get('keyword', '').strip()
        tab = data.get('tab', 'summary')
        auto_analyze = data.get('auto_analyze', False)

        if not keyword_name:
            return JsonResponse({'success': False, 'message': 'Please enter a keyword'})

        game = get_object_or_404(Game, pk=game_id)
        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()

        if not keyword:
            return JsonResponse({
                'success': False,
                'message': f'Keyword "{keyword_name}" not found in database'
            })

        # Проверяем, есть ли ключевое слово у этой игры
        if keyword not in game.keywords.all():
            return JsonResponse({
                'success': False,
                'message': f'Keyword "{keyword_name}" is not associated with this game'
            })

        # Удаляем связь с игрой
        game.keywords.remove(keyword)

        # Обновляем кэш игры
        game.update_cached_counts(force=True)

        # УБИРАЕМ весь автоанализ
        # Даже если auto_analyze=True - игнорируем

        return JsonResponse({
            'success': True,
            'message': f'Keyword "{keyword_name}" removed from game',
            'popularity': keyword.game_set.count()
        })

    except Exception as e:
        print(f"✗ ОШИБКА: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': f'Error deleting keyword: {str(e)}'
        })


# Обновляем метод get_current_keywords для поддержки AJAX
@login_required
@user_passes_test(is_staff_or_superuser)
def get_current_keywords(request: HttpRequest, game_id: int):
    try:
        game = get_object_or_404(Game, pk=game_id)
        keywords = list(game.keywords.values_list('name', flat=True).order_by('name'))

        return JsonResponse({
            'success': True,
            'keywords': keywords,
            'count': len(keywords)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': f'Error getting keywords: {str(e)}'
        })

@login_required
@user_passes_test(is_staff_or_superuser)
def delete_keyword(request: HttpRequest, game_id: int):
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

    try:
        import json
        data = json.loads(request.body)
        keyword_name = data.get('keyword', '').strip()
        tab = data.get('tab', 'summary')
        auto_analyze = data.get('auto_analyze', False)

        if not keyword_name:
            return JsonResponse({'success': False, 'message': 'Please enter a keyword'})

        keyword = Keyword.objects.filter(name__iexact=keyword_name).first()

        if not keyword:
            return JsonResponse({
                'success': False,
                'message': f'Keyword "{keyword_name}" not found in database'
            })

        # Получаем список ID всех игр, которые используют это ключевое слово
        game_ids_with_keyword = list(keyword.game_set.values_list('id', flat=True))

        # Сохраняем популярность (сколько игр используют этот ключевое слово)
        popularity = len(game_ids_with_keyword)

        print(f"\n--- Удаление ключевого слова: {keyword_name} (ID: {keyword.id}) ---")
        print(f"Затронуто игр: {popularity}")

        # Удаляем ключевое слово из базы данных
        keyword.delete()
        print(f"✓ Ключевое слово удалено из БД")

        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Очищаем кэш Trie после удаления ключевого слова
        from games.analyze.keyword_trie import KeywordTrieManager
        KeywordTrieManager().clear_cache()
        print(f"✓ Кэш Trie очищен")

        # Обновляем кэш и материализованные векторы для ВСЕХ игр, которые использовали это ключевое слово
        if game_ids_with_keyword:
            # Оптимизация: Используем bulk_update для массового обновления
            games_to_update = []

            # Получаем все игры одним запросом
            games = Game.objects.filter(id__in=game_ids_with_keyword).prefetch_related('keywords')

            for game in games:
                # Пересчитываем количество ключевых слов
                keyword_count = game.keywords.count()

                # ВАЖНО: Обновляем материализованный вектор keyword_ids
                # Получаем актуальные ID ключевых слов
                new_keyword_ids = list(game.keywords.values_list('igdb_id', flat=True))

                # Проверяем, изменились ли данные
                needs_update = (
                        game._cached_keyword_count != keyword_count or
                        set(new_keyword_ids) != set(game.keyword_ids or [])
                )

                if needs_update:
                    # Обновляем кэшированные счетчики
                    game._cached_keyword_count = keyword_count
                    game._cache_updated_at = timezone.now()

                    # Обновляем материализованный вектор
                    game.keyword_ids = new_keyword_ids

                    games_to_update.append(game)

            # Массовое обновление
            if games_to_update:
                Game.objects.bulk_update(
                    games_to_update,
                    ['_cached_keyword_count', '_cache_updated_at', 'keyword_ids'],
                    batch_size=100
                )
                print(f"✓ Обновлены векторы для {len(games_to_update)} игр")
            else:
                print(f"✓ Игры не требуют обновления")
        else:
            print(f"✓ Нет игр для обновления")

        response_data = {
            'success': True,
            'message': f'Keyword "{keyword_name}" deleted successfully',
            'popularity': popularity
        }

        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Добавляем флаг для автоматического анализа
        if auto_analyze:
            response_data['analyze_after_delete'] = True
            response_data['tab'] = tab
            response_data['message'] += ' и выполняется повторный анализ текста'

            # Сохраняем в сессии информацию для выполнения анализа
            request.session[f'auto_analyze_after_delete_{game_id}'] = {
                'tab': tab,
                'keyword_deleted': keyword_name
            }
            print(f"✓ Запланирован автоанализ для игры {game_id}")

        print(f"✓ Готово\n")
        return JsonResponse(response_data)

    except Exception as e:
        print(f"✗ ОШИБКА: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': f'Error deleting keyword: {str(e)}'
        })


@login_required
@user_passes_test(is_staff_or_superuser)
def get_current_keywords(request: HttpRequest, game_id: int):
    try:
        game = get_object_or_404(Game, pk=game_id)
        keywords = list(game.keywords.values_list('name', flat=True).order_by('name'))

        return JsonResponse({
            'success': True,
            'keywords': keywords,
            'count': len(keywords)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': f'Error getting keywords: {str(e)}'
        })


@login_required
@user_passes_test(is_staff_or_superuser)
def get_found_keywords(request: HttpRequest, game_id: int):
    try:
        game = get_object_or_404(Game, pk=game_id)
        tab = request.GET.get('tab', 'summary')

        unsaved_results = request.session.get(f'unsaved_results_{game_id}', {})
        found_items = unsaved_results.get('found_items', {}).get(tab, {})

        keywords = []
        if found_items.get('keywords'):
            for item in found_items['keywords']:
                keywords.append({
                    'name': item['name'],
                    'is_new': item.get('is_new', False)
                })

        return JsonResponse({
            'success': True,
            'keywords': keywords,
            'count': len(keywords)
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': f'Error getting found keywords: {str(e)}'
        })


@login_required
@user_passes_test(is_staff_or_superuser)
def analyze_single_game(request: HttpRequest, game_id: int):
    """Анализ одной игры - ТОЛЬКО комбинированный режим"""

    # Проверяем, нужно ли выполнить автоматический анализ после удаления ключевого слова
    auto_analyze_key = f'auto_analyze_after_delete_{game_id}'
    if auto_analyze_key in request.session:
        auto_analyze_data = request.session.pop(auto_analyze_key)
        tab = auto_analyze_data.get('tab', 'summary')
        keyword_deleted = auto_analyze_data.get('keyword_deleted', '')

        # Добавляем сообщение
        messages.info(request, f'🔄 Выполняем автоматический анализ после удаления ключевого слова "{keyword_deleted}"')

        # Устанавливаем флаг для автоматического анализа
        request.session[f'auto_analyze_{game_id}'] = True

    # ДОБАВЛЯЕМ: Проверяем, нужно ли обновить Trie перед анализом
    # Это происходит при каждом анализе, гарантируя актуальность данных
    if request.method == 'POST' and 'analyze' in request.POST:
        try:
            from games.analyze.keyword_trie import KeywordTrieManager
            # Очищаем кэш Trie перед анализом, чтобы получить актуальные данные
            KeywordTrieManager().clear_cache()
            print(f"✅ Кэш Trie обновлен перед анализом для игры {game_id}")
        except Exception as e:
            print(f"⚠️ Ошибка при обновлении Trie: {e}")

    # Инициализация
    game, original_descriptions, active_tab = _initialize_analysis_context(request, game_id)

    # Обработка POST запросов
    if request.method == 'POST':
        return _handle_post_request(request, game, original_descriptions, active_tab)

    # Подготовка контекста для GET запроса
    context = _prepare_get_context(request, game, original_descriptions, active_tab)

    return render(request, 'games/analyze.html', context)


def find_keywords_in_hyphenated_words(text: str, matches: List[Dict]) -> List[Dict]:
    """
    Ищет ключевые слова внутри составных слов через дефис
    Например, "devil" в "devil-worshipping"
    """
    hyphenated_matches = []
    text_lower = text.lower()

    # Собираем только ключевые слова
    keyword_matches = [m for m in matches if m['category'] == 'keywords']

    # Ищем все слова с дефисами
    import re
    hyphenated_words = re.findall(r'\b[\w]+-[\w]+\b', text)

    for hyphen_word in hyphenated_words:
        hyphen_word_lower = hyphen_word.lower()
        parts = hyphen_word_lower.split('-')

        # Проверяем каждую часть на соответствие ключевым словам
        for part in parts:
            if len(part) >= 3:  # Минимум 3 символа для значимых частей
                # Ищем ключевое слово, соответствующее этой части
                for keyword_match in keyword_matches:
                    if keyword_match['text'].lower() == part:
                        # Нашли совпадение! Находим позицию этой части в тексте
                        pos = text_lower.find(part)
                        while pos != -1:
                            # Проверяем, что это действительно часть составного слова
                            if pos + len(part) <= len(text):
                                # Проверяем соседние символы
                                has_hyphen_before = pos > 0 and text[pos - 1] == '-'
                                has_hyphen_after = pos + len(part) < len(text) and text[pos + len(part)] == '-'

                                if has_hyphen_before or has_hyphen_after:
                                    # Это часть составного слова
                                    hyphenated_matches.append({
                                        'start': pos,
                                        'end': pos + len(part),
                                        'name': keyword_match['name'],
                                        'category': keyword_match['category'],
                                        'class_name': keyword_match['class_name'],
                                        'color': keyword_match['color'],
                                        'text': text[pos:pos + len(part)],
                                        'is_hyphenated_part': True
                                    })

                            # Ищем следующее вхождение
                            pos = text_lower.find(part, pos + 1)

    return hyphenated_matches


def highlight_words_in_html(html_text: str, words_to_highlight: Dict[str, List[Dict]]) -> str:
    """
    Подсвечивает слова в HTML тексте
    УПРОЩЕНО: не обращает внимание на дефисы, рассматривает составные слова как отдельные слова
    """
    if not html_text or not words_to_highlight:
        return html_text

    import re
    import html

    # Создаем копию для изменений
    result_text = html_text

    # Собираем все слова для подсветки в один паттерн
    # Сортируем по длине в обратном порядке, чтобы длинные слова обрабатывались первыми
    words_patterns = []
    for word_lower, criteria_list in words_to_highlight.items():
        if len(word_lower) >= 2:  # Игнорируем слишком короткие слова
            # Экранируем специальные символы в слове
            escaped_word = re.escape(word_lower)
            # Ищем слово как отдельное или с дефисом перед/после
            pattern = re.compile(
                r'\b' + escaped_word + r'(?=\b|\-)',
                re.IGNORECASE
            )
            words_patterns.append((pattern, criteria_list))

    # Сортируем паттерны по длине слова (длинные слова первыми)
    words_patterns.sort(key=lambda x: len(x[0].pattern), reverse=True)

    # Обрабатываем каждое слово
    for pattern, criteria_list in words_patterns:
        if not criteria_list:
            continue

        criteria = criteria_list[0]  # Берем первый критерий

        # Находим все вхождения этого слова
        matches = list(pattern.finditer(result_text))

        # Обрабатываем с конца, чтобы позиции не смещались
        for match in reversed(matches):
            start_pos = match.start()
            matched_text = match.group(0)

            # Проверяем, что не внутри HTML тега
            if is_inside_html_tag(result_text, start_pos):
                continue

            # Проверяем, что не внутри другого span
            if is_inside_span(result_text, start_pos):
                continue

            # Проверяем границы: если после слова идет дефис, это нормально
            # Если после слова идет буква (не дефис), то пропускаем
            end_pos = start_pos + len(matched_text)
            if end_pos < len(result_text):
                next_char = result_text[end_pos]
                if next_char.isalpha() and next_char not in "s'-":
                    # Это часть более длинного слова, пропускаем
                    continue

            # Создаем подсветку
            highlighted_span = create_single_criteria_span(matched_text, criteria)

            # Заменяем
            result_text = (
                    result_text[:start_pos] +
                    highlighted_span +
                    result_text[start_pos + len(matched_text):]
            )

    return result_text


def create_single_criteria_span(text: str, criteria: Dict) -> str:
    """
    Создает span для одиночного критерия
    """
    category_display = {
        'genres': 'Genre',
        'themes': 'Theme',
        'perspectives': 'Perspective',
        'game_modes': 'Game Mode',
        'keywords': 'Keyword'
    }.get(criteria['category'], criteria['category'])

    return f'<span class="{criteria["class_name"]}" ' \
           f'data-element-name="{html.escape(criteria["name"])}" ' \
           f'data-category="{criteria["category"]}" ' \
           f'title="{category_display}: {html.escape(criteria["name"])}">' \
           f'{text}</span>'


def is_inside_span(html_text: str, position: int) -> bool:
    """
    Проверяет, находится ли позиция внутри span тега
    """
    # Находим последний открывающий span до этой позиции
    last_span_start = html_text.rfind('<span', 0, position)
    if last_span_start == -1:
        return False

    # Находим закрывающий '>' для этого span
    span_end_tag = html_text.find('>', last_span_start)
    if span_end_tag == -1:
        return False

    # Находим закрывающий </span> для этого span
    closing_span = html_text.find('</span>', span_end_tag)
    if closing_span == -1:
        return False

    # Если позиция между началом и концом span, значит внутри
    return span_end_tag < position < closing_span


def get_words_to_highlight(pattern_info: Dict) -> Dict[str, List[Dict]]:
    """
    Собирает слова для подсветки и информацию о критериях
    """
    words_to_highlight = {}
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    category_classes = {
        'genres': 'highlight-genre',
        'themes': 'highlight-theme',
        'perspectives': 'highlight-perspective',
        'game_modes': 'highlight-game_mode',
        'keywords': 'highlight-keyword'
    }

    for category in categories:
        category_matches = pattern_info.get(category, [])
        for match_info in category_matches:
            if match_info.get('status') != 'found':
                continue

            matched_text = match_info.get('matched_text', '')
            name = match_info.get('name', '')

            if not matched_text or not name:
                continue

            # Очищаем текст
            clean_text = matched_text.strip().lower()

            if len(clean_text) >= 2:
                if clean_text not in words_to_highlight:
                    words_to_highlight[clean_text] = []

                # Добавляем информацию о критерии
                words_to_highlight[clean_text].append({
                    'name': name,
                    'category': category,
                    'class_name': category_classes[category],
                    'full_phrase': matched_text,
                    'is_phrase': ' ' in clean_text,
                    'exact_match': clean_text
                })

    return words_to_highlight


def highlight_hyphenated_words(html_text: str, words_to_highlight: Dict[str, List[Dict]]) -> str:
    """
    Специальная функция для подсветки ключевых слов внутри составных слов через дефис
    """
    highlighted_text = html_text

    # Ищем все составные слова через дефис в тексте
    import re
    # Ищем слова с дефисами, сохраняем позиции
    hyphenated_pattern = re.compile(r'(\b[\w]+-[\w]+(?:\-[\w]+)*\b)')

    # Начинаем с конца текста, чтобы позиции не смещались при замене
    matches = list(hyphenated_pattern.finditer(html_text))

    for match in reversed(matches):  # Идем с конца к началу
        hyphenated_word = match.group(1)
        start_pos = match.start()
        end_pos = match.end()

        # Разбиваем на части
        parts = hyphenated_word.split('-')

        # Проверяем каждую часть начиная с конца
        part_end = end_pos
        for i in range(len(parts) - 1, -1, -1):
            part = parts[i]
            part_lower = part.lower()

            # Начало части вычисляем от конца слова
            part_start = start_pos
            for j in range(i):
                part_start += len(parts[j]) + 1  # +1 для дефиса

            part_end = part_start + len(part)

            if part_lower in words_to_highlight:
                # Нашли ключевое слово в части составного слова
                criteria_list = words_to_highlight[part_lower]

                # Получаем оригинальный текст части (с правильным регистром)
                original_part = html_text[part_start:part_end]

                # Проверяем, что еще не подсвечено
                if not is_inside_span(highlighted_text, part_start):
                    # Создаем подсветку для первого критерия
                    if criteria_list:
                        criteria = criteria_list[0]

                        # Создаем подсвеченную версию
                        highlighted_part = create_single_criteria_span(original_part, criteria)

                        # Заменяем часть на подсвеченную версию
                        highlighted_text = (
                                highlighted_text[:part_start] +
                                highlighted_part +
                                highlighted_text[part_end:]
                        )

    return highlighted_text


def create_simple_highlighted_text(html_text: str, analysis_result: Dict) -> str:
    """
    ПРОСТОЙ ВАРИАНТ: ищем и заменяем слова прямо в HTML
    """
    if not html_text or not analysis_result.get('pattern_info'):
        return html_text

    try:
        import re
        import html

        result_text = html_text

        # Собираем все слова для подсветки
        words_to_highlight = {}
        categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
        category_classes = {
            'genres': 'highlight-genre',
            'themes': 'highlight-theme',
            'perspectives': 'highlight-perspective',
            'game_modes': 'highlight-game_mode',
            'keywords': 'highlight-keyword'
        }

        for category in categories:
            category_matches = analysis_result.get('pattern_info', {}).get(category, [])
            for match_info in category_matches:
                if match_info.get('status') == 'found':
                    matched_text = match_info.get('matched_text', '')
                    name = match_info.get('name', '')

                    if matched_text and name:
                        word_lower = matched_text.lower()
                        if word_lower not in words_to_highlight:
                            words_to_highlight[word_lower] = {
                                'name': name,
                                'category': category,
                                'class_name': category_classes[category],
                                'patterns': []
                            }

                        # Создаем паттерн для поиска
                        # Ищем слово как отдельное, или с дефисом после
                        pattern = re.compile(
                            r'\b' + re.escape(word_lower) + r'(?=\b|\-|s\b|\'s\b)',
                            re.IGNORECASE
                        )
                        words_to_highlight[word_lower]['patterns'].append(pattern)

        if not words_to_highlight:
            return html_text

        # Обрабатываем каждое слово
        for word_data in words_to_highlight.values():
            for pattern in word_data['patterns']:
                # Находим все совпадения
                matches = list(pattern.finditer(result_text))

                # Обрабатываем с конца
                for match in reversed(matches):
                    start_pos = match.start()
                    matched_text = match.group(0)

                    # Проверяем, что не внутри HTML тега
                    if is_inside_html_tag(result_text, start_pos):
                        continue

                    # Проверяем, что не внутри другого span
                    if is_inside_span(result_text, start_pos):
                        continue

                    # Создаем span
                    category_display = {
                        'genres': 'Genre',
                        'themes': 'Theme',
                        'perspectives': 'Perspective',
                        'game_modes': 'Game Mode',
                        'keywords': 'Keyword'
                    }.get(word_data['category'], word_data['category'])

                    highlighted_span = (
                        f'<span class="{word_data["class_name"]}" '
                        f'data-element-name="{html.escape(word_data["name"])}" '
                        f'data-category="{word_data["category"]}" '
                        f'title="{category_display}: {html.escape(word_data["name"])}">'
                        f'{matched_text}'
                        f'</span>'
                    )

                    # Заменяем
                    result_text = (
                            result_text[:start_pos] +
                            highlighted_span +
                            result_text[start_pos + len(matched_text):]
                    )

        return result_text

    except Exception as e:
        print(f"❌ Error in create_simple_highlighted_text: {e}")
        import traceback
        traceback.print_exc()
        return html_text


def _initialize_analysis_context(request: HttpRequest, game_id: int) -> Tuple[Game, Dict, str]:
    """Инициализация контекста анализа"""
    game = get_object_or_404(Game, pk=game_id)

    # Получаем оригинальные описания из базы
    original_descriptions = {
        'summary': game.summary or '',
        'storyline': game.storyline or '',
        'rawg': game.rawg_description or '',
        'wiki': game.wiki_description or '',
    }

    # Фильтруем непустые описания
    available_descriptions = {k: v for k, v in original_descriptions.items() if v.strip()}

    # Определяем активную вкладку
    active_tab = request.GET.get('tab', 'summary')
    if active_tab not in available_descriptions:
        active_tab = next(iter(available_descriptions.keys()), 'summary')

    return game, original_descriptions, active_tab


def _handle_save_results(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка сохранения результатов"""
    unsaved_results = request.session.get(f'unsaved_results_{game.id}', {})
    save_data = unsaved_results.get('save_data', {})

    if not save_data:
        messages.error(request, '❌ No analysis data to save. Perform analysis first.')
        return _redirect_to_tab(game.id, active_tab)

    try:
        analyzer = GameAnalyzerAPI(verbose=True)

        # Применяем результаты к базе данных
        update_result = analyzer.update_game_with_combined_results(
            game_id=game.id,
            results=save_data['results']
        )

        if update_result['success']:
            game.last_analyzed_date = timezone.now()
            game.save()

            # Очищаем сохраненные результаты из сессии
            if f'unsaved_results_{game.id}' in request.session:
                del request.session[f'unsaved_results_{game.id}']
                request.session.modified = True

            added_count = save_data.get('found_count', 0)
            if added_count > 0:
                messages.success(request,
                                 f'✅ Successfully added {added_count} new elements to the game!')
            else:
                messages.info(request, 'ℹ️ No new elements to add (all found elements already exist in game).')

            # ФИКС: Правильное формирование URL с параметрами
            redirect_url = reverse('analyze_game', args=[game.id])

            # Создаем параметры запроса
            params = []
            if active_tab and active_tab != 'summary':
                params.append(f'tab={active_tab}')
            params.append('saved=1')

            # Добавляем параметры к URL
            if params:
                redirect_url += '?' + '&'.join(params)

            return redirect(redirect_url)
        else:
            messages.error(request,
                           f'❌ Failed to save results: {update_result.get("error", "Unknown error")}')

    except Exception as e:
        messages.error(request, f'❌ Error saving results: {str(e)}')

    return _redirect_to_tab(game.id, active_tab)


def _redirect_to_tab(game_id: int, tab: str):
    """Редирект на указанную вкладку"""
    redirect_url = reverse('analyze_game', args=[game_id])
    if tab and tab != 'summary':
        # ФИКС: Правильное добавление параметра tab
        redirect_url += f'?tab={tab}'
    return redirect(redirect_url)


def _save_analysis_results(request: HttpRequest, game_id: int, tab: str, original_text: str, analysis_result: Dict):
    """Сохранение результатов анализа в сессии"""
    try:
        # Фильтруем дубликаты
        if 'pattern_info' in analysis_result:
            analysis_result['pattern_info'] = filter_duplicate_patterns(analysis_result['pattern_info'])

        # Форматируем текст
        formatted_text = format_text_with_html(original_text)

        # Используем ПРОСТОЙ метод подсветки
        highlighted_text = simple_highlight_text(formatted_text, analysis_result)

        # Сохраняем данные
        game = Game.objects.get(id=game_id)
        found_items = extract_found_items_combined(analysis_result, game)

        session_key = f'unsaved_results_{game_id}'
        unsaved_results = request.session.get(session_key, {
            'highlighted_text': {},
            'found_items': {},
            'analysis_data': {},
            'save_data': {}
        })

        unsaved_results['highlighted_text'][tab] = highlighted_text
        unsaved_results['found_items'][tab] = found_items
        unsaved_results['analysis_data'][tab] = {
            'text_source': tab,
            'results': analysis_result.get('results', {}),
            'summary': analysis_result.get('summary', {}),
            'pattern_info': analysis_result.get('pattern_info', {}),
            'original_text': original_text,
            'formatted_text': formatted_text
        }

        save_data = prepare_save_data(analysis_result, game, tab)
        unsaved_results['save_data'] = save_data

        request.session[session_key] = unsaved_results
        request.session.modified = True

        print(f"✅ Сохранен анализ для игры {game_id}, вкладка {tab}")
        print(f"📊 Найдено новых элементов: {save_data['found_count']}")

    except Exception as e:
        print(f"❌ Ошибка в _save_analysis_results: {e}")
        import traceback
        traceback.print_exc()


def simple_highlight_text(html_text: str, analysis_result: Dict) -> str:
    """
    ПРОСТОЙ метод подсветки текста БЕЗ вложенных тегов
    УЛУЧШЕНО: Подсвечивает ключевые слова внутри составных слов через дефис
    """
    if not html_text or not analysis_result.get('pattern_info'):
        return html_text

    try:
        # Удаляем существующую подсветку
        html_text = remove_existing_highlights(html_text)

        # Собираем ВСЕ совпадения
        all_matches = collect_all_matches(analysis_result['pattern_info'])

        if not all_matches:
            return html_text

        # Получаем чистый текст из HTML для поиска
        clean_text = get_clean_text_from_html(html_text)

        # Находим ВСЕ позиции всех совпадений в ЧИСТОМ тексте
        text_positions = find_all_text_positions(clean_text, all_matches)

        # ДОБАВЛЯЕМ: Поиск ключевых слов внутри составных слов через дефис
        hyphenated_matches = find_keywords_in_hyphenated_words(clean_text, all_matches)
        text_positions.extend(hyphenated_matches)

        if not text_positions:
            return html_text

        # Группируем пересечения в ЧИСТОМ тексте
        clean_groups = group_overlapping_positions(text_positions)

        # Преобразуем группы в формат для замены
        replacements = prepare_replacements_from_groups(clean_text, clean_groups)

        if not replacements:
            return html_text

        # Применяем подсветку к HTML (прямая замена по тексту)
        highlighted_text = apply_direct_highlights(html_text, replacements)

        return highlighted_text

    except Exception as e:
        print(f"❌ Error in simple_highlight_text: {e}")
        import traceback
        traceback.print_exc()
        return html_text


def collect_all_matches(pattern_info: Dict) -> List[Dict]:
    """
    Собирает все совпадения из pattern_info
    УЛУЧШЕНО: Находит ключевые слова внутри составных слов через дефис
    """
    all_matches = []
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    category_classes = {
        'genres': 'highlight-genre',
        'themes': 'highlight-theme',
        'perspectives': 'highlight-perspective',
        'game_modes': 'highlight-game_mode',
        'keywords': 'highlight-keyword'
    }

    category_colors = {
        'genres': 'rgba(40, 167, 69, 0.2)',  # зеленый
        'themes': 'rgba(220, 53, 69, 0.2)',  # красный
        'perspectives': 'rgba(0, 123, 255, 0.2)',  # синий
        'game_modes': 'rgba(111, 66, 193, 0.2)',  # фиолетовый
        'keywords': 'rgba(255, 193, 7, 0.2)',  # желтый
    }

    for category in categories:
        category_matches = pattern_info.get(category, [])
        for match in category_matches:
            if match.get('status') == 'found':
                matched_text = match.get('matched_text', '')
                name = match.get('name', '')

                if matched_text and name:
                    all_matches.append({
                        'text': matched_text,
                        'name': name,
                        'category': category,
                        'class_name': category_classes[category],
                        'color': category_colors[category]
                    })

    return all_matches


def prepare_replacements_from_groups(clean_text: str, clean_groups: List[Dict]) -> List[Dict]:
    """
    Подготавливает замены на основе групп
    """
    replacements = []

    for group in clean_groups:
        group_text = clean_text[group['start']:group['end']]

        # Убираем группы, которые слишком длинные (вероятно, ошибка)
        if len(group_text) > 100:
            continue

        # Для групп с множественными критериями
        if len(group['matches']) > 1:
            # Собираем уникальные критерии
            unique_matches = {}
            for match in group['matches']:
                key = f"{match['name']}|{match['category']}"
                if key not in unique_matches:
                    unique_matches[key] = match

            if len(unique_matches) > 1:
                # Множественные критерии
                replacements.append({
                    'type': 'multi',
                    'text': group_text,
                    'matches': list(unique_matches.values()),
                    'num_criteria': len(unique_matches)
                })
            else:
                # Один уникальный критерий
                match = list(unique_matches.values())[0]
                replacements.append({
                    'type': 'single',
                    'text': group_text,
                    'match': match
                })
        else:
            # Одиночный критерий
            replacements.append({
                'type': 'single',
                'text': group_text,
                'match': group['matches'][0]
            })

    # Сортируем по длине текста (от самых длинных)
    replacements.sort(key=lambda x: len(x['text']), reverse=True)

    return replacements


def apply_direct_highlights(html_text: str, replacements: List[Dict]) -> str:
    """
    Заменяет текст в HTML на основе УЖЕ НАЙДЕННЫХ и ПРОВЕРЕННЫХ совпадений
    Использует исходный текст для поиска, затем применяет все замены за один проход
    """
    if not replacements:
        return html_text

    # Используем ОРИГИНАЛЬНЫЙ текст для поиска всех позиций
    # Собираем все позиции для замены из исходного текста
    all_positions = []

    for replacement in replacements:
        search_text = replacement['text']

        # Ищем все вхождения этого текста в ОРИГИНАЛЬНОМ HTML
        start_pos = 0

        while True:
            # Регистронезависимый поиск
            found_pos = html_text.lower().find(search_text.lower(), start_pos)
            if found_pos == -1:
                break

            # Проверяем границы слова в ОРИГИНАЛЬНОМ тексте
            is_valid = True

            # Проверяем начало
            if found_pos > 0:
                prev_char = html_text[found_pos - 1]
                if prev_char.isalnum():
                    is_valid = False

            # Проверяем конец
            end_pos = found_pos + len(search_text)
            if end_pos < len(html_text):
                next_char = html_text[end_pos]
                if next_char.isalnum() and next_char != 's' and next_char != '-':
                    is_valid = False

            if is_valid:
                all_positions.append({
                    'start': found_pos,
                    'end': end_pos,
                    'replacement': replacement,
                    'text': html_text[found_pos:end_pos]
                })

            start_pos = found_pos + 1

    if not all_positions:
        return html_text

    # Сортируем позиции от конца к началу для корректной замены
    all_positions.sort(key=lambda x: x['start'], reverse=True)

    # Применяем все замены за один проход
    highlighted_text = html_text

    for pos_info in all_positions:
        try:
            start = pos_info['start']
            end = pos_info['end']
            replacement = pos_info['replacement']
            original_text = pos_info['text']

            # Пропускаем, если позиции вышли за границы (после предыдущих замен)
            if start < 0 or end > len(highlighted_text) or start >= end:
                continue

            # Проверяем, что на этой позиции все еще тот же текст
            current_text = highlighted_text[start:end]
            if current_text.lower() != original_text.lower():
                continue

            # Создаем подсветку
            if replacement['type'] == 'multi':
                span_html = create_multi_highlight_span(original_text, replacement['matches'],
                                                        replacement['num_criteria'])
            else:
                span_html = create_single_highlight_span(original_text, replacement['match'])

            # Заменяем
            highlighted_text = highlighted_text[:start] + span_html + highlighted_text[end:]

        except Exception:
            continue

    return highlighted_text


def create_single_highlight_span(text: str, match: Dict) -> str:
    """
    Создает span для одиночного критерия
    """
    category_display = {
        'genres': 'Genre',
        'themes': 'Theme',
        'perspectives': 'Perspective',
        'game_modes': 'Game Mode',
        'keywords': 'Keyword'
    }.get(match['category'], match['category'])

    title = f"{category_display}: {html.escape(match['name'])}"

    return f'<span class="{match["class_name"]}" ' \
           f'data-element-name="{html.escape(match["name"])}" ' \
           f'data-category="{match["category"]}" ' \
           f'title="{title}">{html.escape(text)}</span>'


def create_multi_highlight_span(text: str, matches: List[Dict], num_criteria: int) -> str:
    """
    Создает span для МНОЖЕСТВЕННЫХ критериев
    """
    # Собираем информацию
    category_names = {
        'genres': 'Genre',
        'themes': 'Theme',
        'perspectives': 'Perspective',
        'game_modes': 'Game Mode',
        'keywords': 'Keyword'
    }

    tooltip_parts = []
    names_list = []
    categories_list = []
    all_classes = set()

    for match in matches:
        category_display = category_names.get(match['category'], match['category'])
        tooltip_parts.append(f"{category_display}: {html.escape(match['name'])}")
        names_list.append(match['name'])
        categories_list.append(match['category'])
        all_classes.add(match['class_name'])

    tooltip_text = " | ".join(tooltip_parts)
    class_str = ' '.join(all_classes)

    # Определяем стиль полосатой подсветки
    if num_criteria == 2:
        striped_style = "background: repeating-linear-gradient(45deg, rgba(220, 53, 69, 0.2), rgba(220, 53, 69, 0.2) 5px, rgba(255, 193, 7, 0.2) 5px, rgba(255, 193, 7, 0.2) 10px) !important;"
    elif num_criteria >= 3:
        striped_style = "background: repeating-linear-gradient(45deg, rgba(220, 53, 69, 0.15), rgba(220, 53, 69, 0.15) 4px, rgba(255, 193, 7, 0.15) 4px, rgba(255, 193, 7, 0.15) 8px, rgba(0, 123, 255, 0.15) 8px, rgba(0, 123, 255, 0.15) 12px) !important;"
    else:
        striped_style = ""

    return f'<span class="highlight-multi {class_str}" ' \
           f'style="{striped_style}" ' \
           f'data-element-names="{html.escape(",".join(names_list))}" ' \
           f'data-categories="{",".join(categories_list)}" ' \
           f'title="{tooltip_text}">{html.escape(text)}</span>'


def get_clean_text_from_html(html_text: str) -> str:
    """
    Извлекает чистый текст из HTML
    """
    import re
    from html import unescape

    # Удаляем HTML теги
    clean = re.sub(r'<[^>]+>', ' ', html_text)
    # Декодируем HTML entities
    clean = unescape(clean)
    # Заменяем множественные пробелы на один
    clean = re.sub(r'\s+', ' ', clean)

    return clean.strip()


def find_all_text_positions(clean_text: str, all_matches: List[Dict]) -> List[Dict]:
    """
    Находит все позиции всех совпадений в тексте С ПРОВЕРКОЙ ГРАНИЦ
    УЛУЧШЕНО: Находит ключевые слова внутри составных слов через дефис
    """
    text_positions = []
    text_lower = clean_text.lower()

    for match in all_matches:
        search_text = match['text'].lower()

        # Пропускаем слишком короткие тексты
        if len(search_text) < 2:
            continue

        # Ищем ВСЕ вхождения этого текста
        pos = 0
        while True:
            found_pos = text_lower.find(search_text, pos)
            if found_pos == -1:
                break

            # ПРОВЕРЯЕМ ГРАНИЦЫ СЛОВА (с поддержкой дефисов)
            is_valid = True

            # Проверяем начало
            if found_pos > 0:
                prev_char = clean_text[found_pos - 1]
                # Допускаем дефис перед словом (часть составного слова)
                if prev_char.isalnum() and prev_char != '-':
                    is_valid = False

            # Проверяем конец
            end_pos = found_pos + len(search_text)
            if end_pos < len(clean_text):
                next_char = clean_text[end_pos]
                # Допускаем дефис после слова (часть составного слова)
                if next_char.isalnum() and next_char not in "s'-":
                    is_valid = False
                # Разрешаем 's' для множественного числа
                elif next_char == 's':
                    # Проверяем границу после 's'
                    if end_pos + 1 < len(clean_text) and clean_text[end_pos + 1].isalnum():
                        is_valid = False

            if is_valid:
                # ДЛЯ КЛЮЧЕВЫХ СЛОВ: особый случай - можем быть частью составного слова
                if match['category'] == 'keywords':
                    # Проверяем, является ли это частью составного слова через дефис
                    # "devil" в "devil-worshipping" - допустимо
                    if found_pos > 0 and clean_text[found_pos - 1] == '-':
                        # Слово после дефиса - валидно
                        is_valid = True
                    if end_pos < len(clean_text) and clean_text[end_pos] == '-':
                        # Слово перед дефисом - валидно
                        is_valid = True

                if is_valid:
                    # Создаем запись с расширенным текстом
                    extended_text = clean_text[found_pos:end_pos]

                    text_positions.append({
                        'start': found_pos,
                        'end': end_pos,
                        'name': match['name'],
                        'category': match['category'],
                        'class_name': match['class_name'],
                        'color': match['color'],
                        'text': extended_text
                    })

            pos = found_pos + 1

    return text_positions


def group_overlapping_positions(text_positions: List[Dict]) -> List[Dict]:
    """
    Группирует пересекающиеся позиции
    """
    if not text_positions:
        return []

    # Сортируем по начальной позиции
    text_positions.sort(key=lambda x: x['start'])

    groups = []
    i = 0

    while i < len(text_positions):
        current = text_positions[i]
        group = {
            'start': current['start'],
            'end': current['end'],
            'matches': [current]
        }

        j = i + 1
        while j < len(text_positions):
            next_pos = text_positions[j]

            # Проверяем пересечение
            if next_pos['start'] < group['end']:
                # Пересекается - расширяем группу
                group['end'] = max(group['end'], next_pos['end'])
                group['matches'].append(next_pos)
                j += 1
            else:
                break

        groups.append(group)
        i = j

    return groups


def apply_highlights_to_html(html_text: str, clean_text: str, groups: List[Dict]) -> str:
    """
    Применяет подсветку к HTML тексту
    """
    # Сначала конвертируем позиции в чистом тексте в позиции в HTML
    html_groups = convert_to_html_positions(html_text, clean_text, groups)

    if not html_groups:
        return html_text

    # Сортируем от конца к началу для замены
    html_groups.sort(key=lambda x: x['html_start'], reverse=True)

    highlighted_text = html_text

    for group in html_groups:
        try:
            start = group['html_start']
            end = group['html_end']

            if start < 0 or end > len(highlighted_text) or start >= end:
                continue

            original_html = highlighted_text[start:end]

            # Пропускаем, если уже содержит span
            if '<span' in original_html and '</span>' in original_html:
                continue

            # Определяем подсветку
            if len(group['matches']) > 1:
                # МНОЖЕСТВЕННЫЕ критерии
                span_html = create_multi_highlight_span(original_html, group['matches'])
            else:
                # Одиночный критерий
                match = group['matches'][0]
                span_html = create_single_highlight_span(original_html, match)

            # Заменяем
            highlighted_text = highlighted_text[:start] + span_html + highlighted_text[end:]

        except Exception as e:
            print(f"Ошибка подсветки: {e}")
            continue

    return highlighted_text


def convert_to_html_positions(html_text: str, clean_text: str, groups: List[Dict]) -> List[Dict]:
    """
    Конвертирует позиции в чистому тексту в позиции в HTML
    """
    html_groups = []

    for group in groups:
        # Находим текст из группы
        group_text = clean_text[group['start']:group['end']]

        # Ищем этот текст в HTML
        html_start = find_text_in_html(html_text, group_text, 0)

        if html_start != -1:
            html_end = html_start + len(group_text)

            # Проверяем, что не внутри тега
            if not is_inside_html_tag(html_text, html_start):
                html_groups.append({
                    'html_start': html_start,
                    'html_end': html_end,
                    'matches': group['matches']
                })

    return html_groups


def find_text_in_html(html_text: str, search_text: str, start_pos: int = 0) -> int:
    """
    Находит текст в HTML (игнорируя теги)
    """
    # Простой поиск - ищем текст как есть
    pos = html_text.find(search_text, start_pos)

    # Если не нашли, пробуем найти с учетом возможных HTML entities
    if pos == -1 and '&' in html_text:
        # Заменяем специальные символы
        decoded_search = html.unescape(search_text)
        pos = html_text.find(decoded_search, start_pos)

    return pos


def highlight_phrases_in_html(html_text: str, words_to_highlight: Dict[str, List[Dict]]) -> str:
    """
    Подсвечивает фразы из нескольких слов
    """
    # Собираем все фразы
    phrases = {}
    for word, criteria_list in words_to_highlight.items():
        for criteria in criteria_list:
            if criteria['is_phrase']:
                phrase = criteria['full_phrase'].lower()
                if phrase not in phrases:
                    phrases[phrase] = []
                phrases[phrase].append(criteria)

    # Сортируем фразы по длине (от самых длинных)
    sorted_phrases = sorted(phrases.items(), key=lambda x: len(x[0]), reverse=True)

    highlighted_text = html_text

    for phrase, criteria_list in sorted_phrases:
        # Ищем фразу в тексте (регистронезависимо)
        pattern = re.compile(re.escape(phrase), re.IGNORECASE)

        def replace_phrase(match):
            matched_text = match.group(0)

            # Проверяем, что это отдельная фраза (не часть слова)
            start_pos = match.start()
            end_pos = match.end()

            if start_pos > 0 and highlighted_text[start_pos - 1].isalnum():
                return matched_text  # Часть слова, пропускаем

            if end_pos < len(highlighted_text) and highlighted_text[end_pos].isalnum():
                return matched_text  # Часть слова, пропускаем

            # Определяем стиль подсветки
            if len(criteria_list) > 1:
                # Множественные критерии
                return create_multi_criteria_span(matched_text, criteria_list)
            else:
                # Одиночный критерий
                criteria = criteria_list[0]
                return create_single_criteria_span(matched_text, criteria)

        # Заменяем фразы
        highlighted_text = pattern.sub(replace_phrase, highlighted_text)

    return highlighted_text


def highlight_single_words_in_html(html_text: str, words_to_highlight: Dict[str, List[Dict]]) -> str:
    """
    Подсвечивает слова в HTML тексте с точным соответствием
    УЛУЧШЕНО: подсвечивает слова внутри составных слов через дефис
    """
    highlighted_text = html_text

    # Группируем по точному тексту для поиска
    exact_matches = {}
    for word, criteria_list in words_to_highlight.items():
        for criteria in criteria_list:
            if not criteria.get('is_phrase', False):
                exact_text = criteria.get('exact_match', word)
                if exact_text not in exact_matches:
                    exact_matches[exact_text] = []
                exact_matches[exact_text].append(criteria)

    # Сортируем по длине (сначала длинные слова)
    sorted_exact_texts = sorted(exact_matches.keys(), key=len, reverse=True)

    for exact_text in sorted_exact_texts:
        criteria_list = exact_matches[exact_text]

        # Ищем точное совпадение (учитывая дефисы и апострофы после слова)
        pattern = re.compile(re.escape(exact_text) + r'(?=[\s\.,;:!\?\'\"\)\]\}]|\-|$)', re.IGNORECASE)

        def replace_exact(match):
            matched_text = match.group(0)
            start_pos = match.start()

            # Проверяем, что не внутри HTML тега
            if is_inside_html_tag(highlighted_text, start_pos):
                return matched_text

            # Проверяем, что не внутри другого span
            if is_inside_span(highlighted_text, start_pos):
                return matched_text

            # Проверяем начало слова
            if start_pos > 0:
                prev_char = highlighted_text[start_pos - 1]
                # Допускаем дефис, пробел, скобку перед словом
                if prev_char.isalnum() and prev_char not in "-'(\"":
                    return matched_text

            if len(criteria_list) > 1:
                # Множественные критерии
                return create_multi_criteria_span(matched_text, criteria_list)
            else:
                # Одиночный критерий
                criteria = criteria_list[0]
                return create_single_criteria_span(matched_text, criteria)

        # Заменяем точные совпадения
        highlighted_text = pattern.sub(replace_exact, highlighted_text)

    return highlighted_text


def create_multi_criteria_span(text: str, criteria_list: List[Dict]) -> str:
    """
    Создает span для множественных критериев
    """
    # Собираем уникальные критерии
    unique_criteria = {}
    for criteria in criteria_list:
        key = f"{criteria['name']}|{criteria['category']}"
        if key not in unique_criteria:
            unique_criteria[key] = criteria

    # Собираем информацию для тултипа
    category_names = {
        'genres': 'Genre',
        'themes': 'Theme',
        'perspectives': 'Perspective',
        'game_modes': 'Game Mode',
        'keywords': 'Keyword'
    }

    tooltip_parts = []
    names_list = []
    categories_list = []
    all_classes = set()

    for criteria in unique_criteria.values():
        category_display = category_names.get(criteria['category'], criteria['category'])
        tooltip_parts.append(f"{category_display}: {html.escape(criteria['name'])}")
        names_list.append(criteria['name'])
        categories_list.append(criteria['category'])
        all_classes.add(criteria['class_name'])

    tooltip_text = " | ".join(tooltip_parts)
    class_str = ' '.join(all_classes)
    num_criteria = len(unique_criteria)

    return f'<span class="highlight-multi {class_str} highlight-striped-{min(num_criteria, 5)}" ' \
           f'data-element-names="{html.escape(",".join(names_list))}" ' \
           f'data-categories="{",".join(categories_list)}" ' \
           f'data-num-criteria="{num_criteria}" ' \
           f'title="{tooltip_text}">' \
           f'{text}</span>'


def filter_duplicate_patterns(pattern_info: Dict) -> Dict:
    """
    Фильтрует дублирующиеся паттерны
    """
    if not pattern_info:
        return {}

    filtered_info = {}

    for category, matches in pattern_info.items():
        if not matches:
            filtered_info[category] = []
            continue

        unique_matches = []
        seen_keys = set()

        for match in matches:
            if match.get('status') != 'found':
                unique_matches.append(match)
                continue

            # Уникальный ключ
            name = match.get('name', '')
            matched_text = match.get('matched_text', '')
            key = f"{name.lower()}_{matched_text.lower()}"

            if key not in seen_keys:
                seen_keys.add(key)
                unique_matches.append(match)

        filtered_info[category] = unique_matches

    return filtered_info


def format_text_with_html(text: str) -> str:
    """
    Форматирует текст в HTML
    """
    if not text:
        return ""

    # Если уже есть HTML
    if '<p>' in text:
        return text

    # Разбиваем на абзацы
    paragraphs = text.split('\n\n')
    formatted = []

    for para in paragraphs:
        if para.strip():
            # Экранируем HTML
            safe_para = html.escape(para.strip())
            # Сохраняем переносы строк внутри абзаца
            safe_para = safe_para.replace('\n', '<br>')
            formatted.append(f'<p>{safe_para}</p>')

    if formatted:
        return '\n'.join(formatted)
    else:
        safe_text = html.escape(text.replace('\n', '<br>'))
        return f'<p>{safe_text}</p>'


def create_highlighted_html(html_text: str, analysis_result: Dict) -> str:
    """
    Создает подсвеченный HTML текст на основе результатов анализа
    Работает с уже отформатированным HTML
    """
    if not html_text or not analysis_result.get('pattern_info'):
        return html_text

    try:
        # Получаем все совпадения в HTML тексте
        html_matches = find_matches_in_formatted_html(html_text, analysis_result.get('pattern_info', {}))

        if not html_matches:
            return html_text

        # Находим пересечения
        overlapping_groups = find_overlapping_matches(html_matches)

        # Создаем список всех позиций для замены (с конца к началу)
        replacements = prepare_replacements(html_text, html_matches, overlapping_groups)

        # Применяем подсветку
        highlighted_text = apply_highlights(html_text, replacements)

        return highlighted_text

    except Exception as e:
        print(f"❌ Error creating highlighted HTML: {e}")
        return html_text


def find_matches_in_formatted_html(html_text: str, pattern_info: Dict) -> List[Dict]:
    """
    Находит совпадения в уже отформатированном HTML тексте
    Возвращает список совпадений с позициями в HTML
    """
    all_matches = []
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    category_classes = {
        'genres': 'highlight-genre',
        'themes': 'highlight-theme',
        'perspectives': 'highlight-perspective',
        'game_modes': 'highlight-game_mode',
        'keywords': 'highlight-keyword'
    }

    # Получаем чистый текст из HTML (для поиска)
    import re
    from html import unescape

    # Удаляем HTML теги, но сохраняем entities
    clean_text = re.sub(r'<[^>]+>', ' ', html_text)  # Заменяем теги на пробелы
    clean_text = unescape(clean_text)  # Декодируем HTML entities
    clean_text = ' '.join(clean_text.split())  # Нормализуем пробелы

    for category in categories:
        category_matches = pattern_info.get(category, [])
        for match_info in category_matches:
            if match_info.get('status') != 'found':
                continue

            matched_text = match_info.get('matched_text', '')
            name = match_info.get('name', '')

            if not matched_text or not name:
                continue

            # Ищем в чистом тексте
            search_text_lower = matched_text.lower()
            clean_text_lower = clean_text.lower()

            pos = 0
            while True:
                found_pos = clean_text_lower.find(search_text_lower, pos)
                if found_pos == -1:
                    break

                # Находим соответствующие позиции в HTML
                html_positions = find_html_positions_for_match(
                    html_text, clean_text, found_pos, len(matched_text)
                )

                if html_positions:
                    html_start, html_end = html_positions

                    # Проверяем, что не внутри существующего span
                    if not is_inside_existing_span(html_text, html_start):
                        all_matches.append({
                            'html_start': html_start,
                            'html_end': html_end,
                            'name': name,
                            'category': category,
                            'class_name': category_classes[category],
                            'matched_text': matched_text,
                            'html_fragment': html_text[html_start:html_end]
                        })

                pos = found_pos + 1

    return all_matches


def find_html_positions_for_match(html_text: str, clean_text: str,
                                  clean_start: int, length: int) -> Optional[Tuple[int, int]]:
    """
    Находит позиции в HTML для совпадения в чистом тексте
    """
    if clean_start < 0 or clean_start + length > len(clean_text):
        return None

    # Получаем текст для поиска
    search_text = clean_text[clean_start:clean_start + length]

    # Ищем этот текст в HTML, игнорируя теги
    html_pos = 0
    clean_index = 0
    in_tag = False
    in_entity = False
    potential_start = -1

    while html_pos < len(html_text) and clean_index < len(clean_text):
        char = html_text[html_pos]

        if char == '<':
            in_tag = True
            html_pos += 1
            continue
        elif char == '>':
            in_tag = False
            html_pos += 1
            continue
        elif char == '&' and not in_tag:
            in_entity = True
            entity_end = html_text.find(';', html_pos)
            if entity_end != -1:
                # Пропускаем entity
                html_pos = entity_end + 1
                clean_index += 1
                in_entity = False
                continue

        if not in_tag and not in_entity:
            if clean_index == clean_start:
                potential_start = html_pos

            if clean_index == clean_start + length - 1 and potential_start != -1:
                return potential_start, html_pos + 1

            if html_text[html_pos] == clean_text[clean_index]:
                clean_index += 1

        html_pos += 1

    return None


def is_inside_existing_span(html_text: str, position: int) -> bool:
    """
    Проверяет, находится ли позиция внутри существующего span
    """
    # Находим последний открывающий span до этой позиции
    last_span_start = html_text.rfind('<span', 0, position)
    if last_span_start == -1:
        return False

    # Находим закрывающий span для этого открывающего
    span_content_start = html_text.find('>', last_span_start)
    if span_content_start == -1:
        return False

    span_end = html_text.find('</span>', span_content_start)
    if span_end == -1:
        return False

    # Проверяем, находится ли позиция между началом и концом span
    return span_content_start < position < span_end


def find_overlapping_matches(matches: List[Dict]) -> Dict:
    """
    Находит пересекающиеся совпадения
    """
    if not matches:
        return {'overlapping': [], 'non_overlapping': []}

    # Сортируем по начальной позиции
    matches.sort(key=lambda x: x['html_start'])

    overlapping_groups = []
    non_overlapping = []

    i = 0
    while i < len(matches):
        current = matches[i]
        group = {
            'start': current['html_start'],
            'end': current['html_end'],
            'matches': [current]
        }

        j = i + 1
        has_overlap = False

        while j < len(matches):
            next_match = matches[j]

            # Проверяем пересечение
            if (current['html_start'] <= next_match['html_start'] < current['html_end'] or
                    current['html_start'] <= next_match['html_end'] < current['html_end'] or
                    next_match['html_start'] <= current['html_start'] < next_match['html_end']):

                # Расширяем группу
                group['start'] = min(group['start'], next_match['html_start'])
                group['end'] = max(group['end'], next_match['html_end'])
                group['matches'].append(next_match)
                has_overlap = True
                j += 1
            else:
                break

        if has_overlap and len(group['matches']) > 1:
            overlapping_groups.append(group)
            i = j
        else:
            non_overlapping.append(current)
            i += 1

    return {
        'overlapping': overlapping_groups,
        'non_overlapping': non_overlapping
    }


def prepare_replacements(html_text: str, matches: List[Dict], overlapping_groups: Dict) -> List[Dict]:
    """
    Подготавливает список замен для применения подсветки
    """
    replacements = []

    # Добавляем группы пересечений
    for group in overlapping_groups['overlapping']:
        matches_in_group = group['matches']
        criteria_types = set(m['category'] for m in matches_in_group)
        num_criteria = len(criteria_types)

        replacements.append({
            'start': group['start'],
            'end': group['end'],
            'type': 'multi',
            'matches': matches_in_group,
            'num_criteria': num_criteria,
            'criteria_types': list(criteria_types),
            'original_html': html_text[group['start']:group['end']]
        })

    # Добавляем одиночные совпадения
    for match in overlapping_groups['non_overlapping']:
        # Проверяем, не входит ли в уже обработанную группу
        in_group = False
        for group in overlapping_groups['overlapping']:
            if match['html_start'] >= group['start'] and match['html_end'] <= group['end']:
                in_group = True
                break

        if not in_group:
            replacements.append({
                'start': match['html_start'],
                'end': match['html_end'],
                'type': 'single',
                'match': match,
                'original_html': html_text[match['html_start']:match['html_end']]
            })

    # Сортируем от конца к началу
    replacements.sort(key=lambda x: x['start'], reverse=True)

    return replacements


def apply_highlights(html_text: str, replacements: List[Dict]) -> str:
    """
    Применяет подсветку к HTML тексту
    """
    highlighted_text = html_text
    category_colors = {
        'genres': '#28a745',
        'themes': '#dc3545',
        'perspectives': '#007bff',
        'game_modes': '#6f42c1',
        'keywords': '#ffc107',
    }

    for replacement in replacements:
        try:
            if replacement['start'] < 0 or replacement['end'] > len(highlighted_text):
                continue

            original_html = replacement['original_html']

            # Пропускаем, если уже содержит span
            if '<span' in original_html and '</span>' in original_html:
                continue

            if replacement['type'] == 'multi':
                # Множественные критерии
                matches = replacement['matches']

                # Собираем уникальные критерии
                unique_criteria = {}
                for match in matches:
                    key = f"{match['name']}|{match['category']}"
                    if key not in unique_criteria:
                        unique_criteria[key] = match

                # Собираем информацию
                category_names = {
                    'genres': 'Genre',
                    'themes': 'Theme',
                    'perspectives': 'Perspective',
                    'game_modes': 'Game Mode',
                    'keywords': 'Keyword'
                }

                tooltip_parts = []
                names_list = []
                categories_list = []

                for match in unique_criteria.values():
                    category_display = category_names.get(match['category'], match['category'])
                    tooltip_parts.append(f"{category_display}: {html.escape(match['name'])}")
                    names_list.append(match['name'])
                    categories_list.append(match['category'])

                tooltip_text = " | ".join(tooltip_parts)

                # Определяем стиль полосатой подсветки
                num_criteria = replacement['num_criteria']
                striped_class = f"highlight-striped-{num_criteria}"

                # Создаем span
                highlighted_span = (
                    f'<span class="highlight-multi {striped_class}" '
                    f'data-element-names="{html.escape(",".join(names_list))}" '
                    f'data-categories="{",".join(categories_list)}" '
                    f'data-num-criteria="{num_criteria}" '
                    f'data-overlap="multi" '
                    f'title="{tooltip_text}">'
                    f'{original_html}'
                    f'</span>'
                )

            else:
                # Одиночный критерий
                match = replacement['match']
                category_display = {
                    'genres': 'Genre',
                    'themes': 'Theme',
                    'perspectives': 'Perspective',
                    'game_modes': 'Game Mode',
                    'keywords': 'Keyword'
                }.get(match['category'], match['category'])

                highlighted_span = (
                    f'<span class="{match["class_name"]}" '
                    f'data-element-name="{html.escape(match["name"])}" '
                    f'data-category="{match["category"]}" '
                    f'data-overlap="single" '
                    f'title="{category_display}: {html.escape(match["name"])}">'
                    f'{original_html}'
                    f'</span>'
                )

            # Применяем замену
            highlighted_text = (
                    highlighted_text[:replacement['start']] +
                    highlighted_span +
                    highlighted_text[replacement['end']:]
            )

        except Exception as e:
            print(f"Error applying highlight at position {replacement.get('start')}: {e}")
            continue

    return highlighted_text


def _prepare_get_context(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str) -> Dict:
    """Подготовка контекста для GET запроса
    ИСПРАВЛЕНИЕ: Упрощенная логика с сохранением абзацной структуры
    """

    # Проверяем флаг auto_analyze в URL
    auto_analyze_flag = request.GET.get('auto_analyze', '0') == '1'

    # Проверяем флаг автоматического анализа после удаления ключевого слова
    auto_analyze_delete_flag = request.session.get(f'auto_analyze_{game.id}', False)
    if auto_analyze_delete_flag:
        request.session.pop(f'auto_analyze_{game.id}', None)
        messages.info(request, '🔍 Текст автоматически проанализирован после удаления ключевого слова.')

    if auto_analyze_flag:
        messages.info(request, '🔍 Текст автоматически проанализирован после добавления ключевого слова.')

    # Получаем сохраненные результаты из сессии
    unsaved_results = request.session.get(f'unsaved_results_{game.id}', {})
    unsaved_highlighted_text = unsaved_results.get('highlighted_text', {}).get(active_tab, '')
    unsaved_found_items = unsaved_results.get('found_items', {}).get(active_tab, {})

    highlight_enabled = request.session.get(f'highlight_enabled_{game.id}', True)

    # ИСПРАВЛЕНИЕ: Используем текст из сессии если есть, иначе форматируем оригинальный
    display_text = ''
    found_items = {}

    if unsaved_highlighted_text and highlight_enabled:
        # Текст уже в HTML формате из сессии, применяем mark_safe
        display_text = mark_safe(unsaved_highlighted_text)
        found_items = unsaved_found_items
    elif active_tab in original_descriptions:
        # Форматируем обычный текст в HTML с сохранением абзацев
        text = original_descriptions[active_tab]
        formatted_text = format_text_with_html(text)
        display_text = mark_safe(formatted_text)

    # Подготавливаем вкладки
    description_tabs = []
    tab_labels = {
        'summary': 'Описание',
        'storyline': 'Сюжет',
        'rawg': 'RAWG',
        'wiki': 'Wikipedia'
    }

    for tab_key, tab_label in tab_labels.items():
        if tab_key in original_descriptions:
            text = original_descriptions[tab_key]
            is_active = (tab_key == active_tab)

            # ИСПРАВЛЕНИЕ: Правильная логика отображения текста для каждой вкладки
            if is_active:
                # Для активной вкладки используем display_text (уже сформирован выше)
                tab_display_text = display_text
            else:
                # Для неактивных вкладок форматируем текст без подсветки
                formatted_text = format_text_with_html(text)
                tab_display_text = mark_safe(formatted_text)

            description_tabs.append({
                'key': tab_key,
                'label': tab_label,
                'length': len(text),
                'is_active': is_active,
                'text': tab_display_text,
                'is_highlighted': (is_active and bool(unsaved_highlighted_text) and highlight_enabled)
            })
        else:
            # Вкладка без текста
            description_tabs.append({
                'key': tab_key,
                'label': tab_label,
                'length': 0,
                'is_active': (tab_key == active_tab),
                'text': mark_safe('<div class="no-highlights text-center py-5">'
                                  '<i class="bi bi-emoji-frown display-4 text-muted mb-3"></i>'
                                  '<h5 class="text-muted mb-2">No Content Available</h5>'
                                  '<p class="text-muted mb-0">This description is empty</p>'
                                  '</div>'),
                'is_highlighted': False
            })

    # Текущие критерии игры
    current_genres = list(game.genres.all())
    current_themes = list(game.themes.all())
    current_perspectives = list(game.player_perspectives.all())
    current_game_modes = list(game.game_modes.all())
    current_keywords = list(game.keywords.all())

    # Проверяем, есть ли несохраненные результаты
    has_unsaved_results = bool(request.session.get(f'unsaved_results_{game.id}', {}).get('save_data'))

    # Подготавливаем контекст
    context = {
        'game': game,
        'description_tabs': description_tabs,
        'current_genres': current_genres,
        'current_themes': current_themes,
        'current_perspectives': current_perspectives,
        'current_game_modes': current_game_modes,
        'current_keywords': current_keywords,
        'analyze_mode': 'combined',
        'found_items': found_items,
        'has_unsaved_results': has_unsaved_results,
        'highlight_enabled': highlight_enabled,
        'active_tab': active_tab,
    }

    return context


def highlight_matches_in_text_combined(text: str, analysis_result: Dict) -> str:
    """
    Улучшенная подсветка с поддержкой множественных критериев на одних и тех же словах
    Работает с уже отформатированным HTML текстом
    """
    if not text or not analysis_result.get('pattern_info'):
        return text if text else ""

    # Текст уже должен быть в HTML формате (с <p> тегами)
    # Работаем с ним как с HTML, а не с чистым текстом

    # Собираем все совпадения из pattern_info
    all_matches = []
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    category_classes = {
        'genres': 'highlight-genre',
        'themes': 'highlight-theme',
        'perspectives': 'highlight-perspective',
        'game_modes': 'highlight-game_mode',
        'keywords': 'highlight-keyword'
    }

    category_colors = {
        'genres': '#28a745',  # зеленый
        'themes': '#dc3545',  # красный
        'perspectives': '#007bff',  # синий
        'game_modes': '#6f42c1',  # фиолетовый
        'keywords': '#ffc107',  # желтый
    }

    # Сначала получаем чистый текст для поиска (без HTML тегов)
    import re
    clean_text = re.sub(r'<[^>]+>', '', text)

    # Собираем все совпадения с их позициями в ЧИСТОМ тексте
    for category in categories:
        category_matches = analysis_result.get('pattern_info', {}).get(category, [])
        for match in category_matches:
            if match.get('status') == 'found':
                matched_text = match.get('matched_text', '')
                if matched_text:
                    # Находим все вхождения в ЧИСТОМ тексте
                    matches_in_text = _find_all_occurrences_in_clean_text(
                        clean_text, matched_text, match.get('name', ''),
                        category, category_classes[category]
                    )
                    all_matches.extend(matches_in_text)

    if not all_matches:
        return text

    # Удаляем дубликаты
    all_matches = _remove_duplicate_matches(all_matches)

    # Преобразуем позиции в чистом тексте в позиции в HTML тексте
    html_matches = _convert_clean_positions_to_html_positions(text, clean_text, all_matches)

    if not html_matches:
        return text

    # Находим пересечения в HTML тексте
    overlapping_groups = _find_overlapping_groups(html_matches)

    # Создаем список всех позиций для замены (с конца к началу)
    all_positions = []

    # Добавляем группы пересечений сначала
    for group in overlapping_groups['overlapping']:
        matches = group['matches']
        criteria_types = set(m['category'] for m in matches)
        num_criteria = len(criteria_types)

        all_positions.append({
            'start': group['start'],
            'end': group['end'],
            'type': 'multi',
            'matches': matches,
            'num_criteria': num_criteria,
            'criteria_types': list(criteria_types),
            'html_text': text[group['start']:group['end']]
        })

    # Добавляем одиночные совпадения
    for match in overlapping_groups['non_overlapping']:
        in_group = False
        for group in overlapping_groups['overlapping']:
            if match['start'] >= group['start'] and match['end'] <= group['end']:
                in_group = True
                break

        if not in_group:
            all_positions.append({
                'start': match['start'],
                'end': match['end'],
                'type': 'single',
                'match': match,
                'html_text': text[match['start']:match['end']]
            })

    # Сортируем от конца к началу для корректной замены
    all_positions.sort(key=lambda x: x['start'], reverse=True)

    # Применяем подсветку к HTML тексту
    highlighted_text = text

    for pos_info in all_positions:
        try:
            if pos_info['start'] < 0 or pos_info['end'] > len(highlighted_text) or \
                    pos_info['start'] >= pos_info['end']:
                continue

            # Получаем оригинальный HTML фрагмент
            original_html = highlighted_text[pos_info['start']:pos_info['end']]

            # Пропускаем, если это уже содержит span (чтобы не вкладывать)
            if '<span' in original_html and '</span>' in original_html:
                continue

            # Пропускаем, если это HTML тег (но не текст)
            if original_html.strip().startswith('<') and original_html.strip().endswith('>'):
                continue

            if pos_info['type'] == 'multi':
                # Множественные критерии
                matches = pos_info['matches']

                # Собираем уникальные критерии
                unique_criteria = {}
                for match in matches:
                    key = f"{match['name']}|{match['category']}"
                    if key not in unique_criteria:
                        unique_criteria[key] = match

                # Собираем информацию для тултипа
                category_names = {
                    'genres': 'Genre',
                    'themes': 'Theme',
                    'perspectives': 'Perspective',
                    'game_modes': 'Game Mode',
                    'keywords': 'Keyword'
                }

                tooltip_parts = []
                names_list = []
                categories_list = []

                for match in unique_criteria.values():
                    category_display = category_names.get(match['category'], match['category'])
                    tooltip_parts.append(f"{category_display}: {html.escape(match['name'])}")
                    names_list.append(match['name'])
                    categories_list.append(match['category'])

                tooltip_text = " | ".join(tooltip_parts)

                # Определяем стиль полосатой подсветки
                num_criteria = pos_info['num_criteria']
                criteria_types = pos_info['criteria_types']

                # Определяем CSS класс для полос
                if num_criteria == 2:
                    striped_class = "highlight-striped-2"
                    # Получаем цвета для двух критериев
                    color1 = category_colors.get(criteria_types[0], '#cccccc')
                    color2 = category_colors.get(criteria_types[1], '#cccccc')
                    style = f"background: linear-gradient(90deg, {color1}22 0%, {color1}22 50%, {color2}22 50%, {color2}22 100%) !important; border-left: 2px solid {color1}44 !important; border-right: 2px solid {color2}44 !important;"
                elif num_criteria == 3:
                    striped_class = "highlight-striped-3"
                    style = ""  # Используем CSS класс
                elif num_criteria == 4:
                    striped_class = "highlight-striped-4"
                    style = ""
                else:
                    striped_class = "highlight-striped-5"
                    style = ""

                # Создаем элемент с полосатой подсветкой
                highlighted_span = (
                    f'<span class="highlight-multi {striped_class}" '
                    f'style="{style}" '
                    f'data-element-names="{html.escape(",".join(names_list))}" '
                    f'data-categories="{",".join(categories_list)}" '
                    f'data-overlap="multi" '
                    f'title="{tooltip_text}">'
                    f'{original_html}'
                    f'</span>'
                )

            else:
                # Одиночный критерий
                match = pos_info['match']
                category_display = {
                    'genres': 'Genre',
                    'themes': 'Theme',
                    'perspectives': 'Perspective',
                    'game_modes': 'Game Mode',
                    'keywords': 'Keyword'
                }.get(match['category'], match['category'])

                # Получаем цвет для этого критерия
                color = category_colors.get(match['category'], '#cccccc')

                highlighted_span = (
                    f'<span class="{match["class_name"]}" '
                    f'style="background-color: {color}22 !important; border-color: {color}44 !important;" '
                    f'data-element-name="{html.escape(match["name"])}" '
                    f'data-category="{match["category"]}" '
                    f'data-overlap="single" '
                    f'title="{category_display}: {html.escape(match["name"])}">'
                    f'{original_html}'
                    f'</span>'
                )

            # Заменяем HTML фрагмент
            highlighted_text = (
                    highlighted_text[:pos_info['start']] +
                    highlighted_span +
                    highlighted_text[pos_info['end']:]
            )

        except Exception as e:
            print(f"Error highlighting HTML at position {pos_info.get('start')}: {e}")
            continue

    return highlighted_text


def _find_matches_in_html_text(html_text: str, pattern_info: Dict) -> List[Dict]:
    """
    Находит совпадения непосредственно в HTML тексте
    Более надежный метод для работы с уже отформатированным текстом
    """
    all_matches = []
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    category_classes = {
        'genres': 'highlight-genre',
        'themes': 'highlight-theme',
        'perspectives': 'highlight-perspective',
        'game_modes': 'highlight-game_mode',
        'keywords': 'highlight-keyword'
    }

    # Получаем чистый текст из HTML
    import re
    clean_text = re.sub(r'<[^>]+>', '', html_text)
    clean_text = html.unescape(clean_text)  # Декодируем HTML entities

    for category in categories:
        category_matches = pattern_info.get(category, [])
        for match in category_matches:
            if match.get('status') == 'found':
                matched_text = match.get('matched_text', '')
                name = match.get('name', '')

                if matched_text and name:
                    # Ищем в чистом тексте
                    search_text = matched_text.lower()
                    text_lower = clean_text.lower()

                    pos = 0
                    while True:
                        found_pos = text_lower.find(search_text, pos)
                        if found_pos == -1:
                            break

                        # Находим соответствующие позиции в HTML
                        html_positions = _find_html_positions_for_clean_text(
                            html_text, clean_text, found_pos, len(search_text)
                        )

                        if html_positions:
                            html_start, html_end = html_positions
                            html_match_text = html_text[html_start:html_end]

                            # Проверяем, что это не внутри другого span
                            if not _is_inside_existing_span(html_text, html_start):
                                all_matches.append({
                                    'start': html_start,
                                    'end': html_end,
                                    'name': name,
                                    'category': category,
                                    'class_name': category_classes[category],
                                    'text': html_match_text
                                })

                        pos = found_pos + 1

    return all_matches


def _find_html_positions_for_clean_text(html_text: str, clean_text: str,
                                        clean_start: int, length: int) -> Tuple[int, int]:
    """
    Находит позиции в HTML тексте для заданного фрагмента в чистом тексте
    """
    # Находим текст в чистом виде
    clean_fragment = clean_text[clean_start:clean_start + length]

    # Ищем этот текст в HTML (игнорируя теги)
    html_pos = 0
    clean_pos = 0
    in_tag = False

    while html_pos < len(html_text) and clean_pos < len(clean_text):
        char = html_text[html_pos]

        if char == '<':
            in_tag = True
        elif char == '>':
            in_tag = False
        elif not in_tag and char == clean_text[clean_pos]:
            if clean_pos == clean_start:
                # Нашли начало
                start_html_pos = html_pos

                # Ищем конец
                end_clean_pos = clean_start + length - 1
                current_html_pos = html_pos
                current_clean_pos = clean_pos

                while (current_html_pos < len(html_text) and
                       current_clean_pos < len(clean_text) and
                       current_clean_pos <= end_clean_pos):

                    current_char = html_text[current_html_pos]

                    if current_char == '<':
                        # Пропускаем тег
                        tag_end = html_text.find('>', current_html_pos)
                        if tag_end != -1:
                            current_html_pos = tag_end + 1
                            continue

                    if not in_tag and current_char == clean_text[current_clean_pos]:
                        if current_clean_pos == end_clean_pos:
                            # Нашли конец
                            return start_html_pos, current_html_pos + 1
                        current_clean_pos += 1

                    current_html_pos += 1

                # Если дошли сюда, что-то пошло не так
                break

            clean_pos += 1

        html_pos += 1

    return None


def _is_inside_existing_span(html_text: str, position: int) -> bool:
    """
    Проверяет, находится ли позиция внутри существующего span
    """
    # Находим последний открывающий span до этой позиции
    span_start = html_text.rfind('<span', 0, position)
    if span_start == -1:
        return False

    # Находим закрывающий span для этого открывающего
    span_end = html_text.find('</span>', span_start)
    if span_end == -1:
        return False

    # Если позиция находится между span_start и span_end, значит внутри span
    return span_start < position < span_end


def _find_all_occurrences_in_clean_text(clean_text: str, search_text: str, name: str,
                                        category: str, class_name: str) -> List[Dict]:
    """
    Находит все вхождения текста в чистом тексте (без HTML)
    УЛУЧШЕНО: находит слова внутри составных слов через дефис
    """
    occurrences = []
    if not search_text or not clean_text:
        return occurrences

    search_lower = search_text.lower()
    text_lower = clean_text.lower()
    search_len = len(search_text)

    pos = 0
    while True:
        found_pos = text_lower.find(search_lower, pos)
        if found_pos == -1:
            break

        # Проверяем границы слова с поддержкой дефисов
        is_valid = True

        # Проверяем начало
        if found_pos > 0:
            prev_char = clean_text[found_pos - 1]
            if prev_char.isalnum() and prev_char != '-':
                is_valid = False

        # Проверяем конец
        end_pos = found_pos + search_len
        if end_pos < len(clean_text):
            next_char = clean_text[end_pos]
            # Допускаем дефис, апостроф, или конец слова
            if next_char.isalnum() and next_char not in "s'-":
                is_valid = False
            # Разрешаем 's' только если дальше не буква
            elif next_char == 's':
                if end_pos + 1 < len(clean_text) and clean_text[end_pos + 1].isalnum():
                    is_valid = False

        if is_valid:
            occurrences.append({
                'start': found_pos,
                'end': end_pos,
                'name': name,
                'category': category,
                'class_name': class_name,
                'text': clean_text[found_pos:end_pos]
            })

        pos = found_pos + 1

    return occurrences


def _convert_clean_positions_to_html_positions(html_text: str, clean_text: str,
                                               clean_matches: List[Dict]) -> List[Dict]:
    """
    Конвертирует позиции в чистом тексте в позиции в HTML тексте
    """
    html_matches = []

    # Создаем карту соответствия позиций
    clean_to_html_map = []
    clean_index = 0

    for i, char in enumerate(html_text):
        if char == '<':
            # Начало HTML тега, пропускаем
            tag_end = html_text.find('>', i)
            if tag_end != -1:
                # Пропускаем весь тег
                i = tag_end
                continue

        if char == '&':
            # HTML entity, пропускаем
            entity_end = html_text.find(';', i)
            if entity_end != -1:
                # Entity считается как один символ в чистом тексте
                clean_to_html_map.append((clean_index, i))
                clean_index += 1
                i = entity_end
                continue

        # Обычный текстовый символ
        if clean_index < len(clean_text) and char == clean_text[clean_index]:
            clean_to_html_map.append((clean_index, i))
            clean_index += 1

    # Теперь конвертируем позиции для каждого совпадения
    for match in clean_matches:
        clean_start = match['start']
        clean_end = match['end']

        # Находим соответствующие позиции в HTML
        html_start = None
        html_end = None

        for clean_pos, html_pos in clean_to_html_map:
            if clean_pos == clean_start:
                html_start = html_pos
            if clean_pos == clean_end - 1:  # -1 потому что end exclusive
                html_end = html_pos + 1  # +1 чтобы включить последний символ

        if html_start is not None and html_end is not None and html_start < html_end:
            # Получаем текст из HTML для проверки
            html_match_text = html_text[html_start:html_end]

            # Убираем HTML теги для проверки
            import re
            text_without_tags = re.sub(r'<[^>]+>', '', html_match_text)

            if text_without_tags.strip():  # Проверяем, что есть реальный текст
                html_matches.append({
                    'start': html_start,
                    'end': html_end,
                    'name': match['name'],
                    'category': match['category'],
                    'class_name': match['class_name'],
                    'text': html_match_text
                })

    return html_matches


def _find_all_occurrences_in_plain_text(plain_text: str, search_text: str, name: str,
                                        category: str, class_name: str) -> List[Dict]:
    """
    Находит все вхождения текста в ПРОСТОМ тексте (без HTML)
    УЛУЧШЕНО: находит слова внутри составных слов через дефис
    """
    occurrences = []
    if not search_text or not plain_text:
        return occurrences

    text_lower = plain_text.lower()
    search_lower = search_text.lower()

    # Регистронезависимый поиск
    search_len = len(search_text)

    # Ищем все вхождения напрямую
    pos = 0
    while True:
        found_pos = text_lower.find(search_lower, pos)
        if found_pos == -1:
            break

        # Проверяем, что найденное слово является частью допустимого контекста:
        # 1. Может быть отдельным словом
        # 2. Может быть частью составного слова через дефис
        # 3. Может иметь после себя 's (притяжательный падеж)

        is_valid = True

        # Проверяем начало
        if found_pos > 0:
            prev_char = plain_text[found_pos - 1]
            # Допускаем пробел, дефис, скобку, кавычку
            if prev_char.isalnum() and prev_char not in "-'(\"":
                is_valid = False

        # Проверяем конец
        end_pos = found_pos + search_len
        if end_pos < len(plain_text):
            next_char = plain_text[end_pos]
            # Допускаем: дефис, апостроф+s, пробел, пунктуация
            if next_char.isalnum():
                # Разрешаем 's для притяжательного падежа
                if next_char == 's' and end_pos + 1 < len(plain_text) and plain_text[end_pos + 1] in " ,.!?;:'\")":
                    # "devil's" - допустимо
                    pass
                # Разрешаем дефис после слова
                elif next_char == '-':
                    # "devil-worshipping" - допустимо
                    pass
                else:
                    is_valid = False

        if is_valid:
            occurrences.append({
                'start': found_pos,
                'end': end_pos,
                'name': name,
                'category': category,
                'class_name': class_name,
                'text': plain_text[found_pos:end_pos]
            })

        pos = found_pos + 1

    return occurrences


def _remove_duplicate_matches(matches: List[Dict]) -> List[Dict]:
    """
    Удаляет дубликаты совпадений (одинаковые позиции и критерии)
    """
    unique_matches = []
    seen = set()

    for match in matches:
        key = (match['start'], match['end'], match['name'], match['category'])
        if key not in seen:
            seen.add(key)
            unique_matches.append(match)

    return unique_matches


def _find_overlapping_groups(matches: List[Dict]) -> Dict:
    """
    Находит группы пересекающихся совпадений
    """
    if not matches:
        return {'overlapping': [], 'non_overlapping': []}

    # Сортируем по начальной позиции
    matches.sort(key=lambda x: x['start'])

    overlapping_groups = []
    non_overlapping = []

    i = 0
    while i < len(matches):
        current = matches[i]
        group = {
            'start': current['start'],
            'end': current['end'],
            'matches': [current]
        }

        j = i + 1
        has_overlap = False

        while j < len(matches):
            next_match = matches[j]

            # Проверяем пересечение
            if (current['start'] <= next_match['start'] < current['end'] or
                    current['start'] <= next_match['end'] < current['end'] or
                    next_match['start'] <= current['start'] < next_match['end']):

                # Расширяем группу
                group['start'] = min(group['start'], next_match['start'])
                group['end'] = max(group['end'], next_match['end'])
                group['matches'].append(next_match)
                has_overlap = True
                j += 1
            else:
                break

        if has_overlap and len(group['matches']) > 1:
            overlapping_groups.append(group)
            i = j
        else:
            non_overlapping.append(current)
            i += 1

    return {
        'overlapping': overlapping_groups,
        'non_overlapping': non_overlapping
    }


def _format_text_with_html(text: str) -> str:
    """
    Преобразует текст в безопасный HTML с сохранением абзацной структуры
    """
    if not text:
        return ""

    # Если текст уже содержит HTML теги, возвращаем как есть
    if '<p>' in text or '<br>' in text or '<div>' in text:
        return text

    # Разделяем на абзацы по двойным переносам строк
    paragraphs = text.split('\n\n')

    formatted_paragraphs = []

    for para in paragraphs:
        if para.strip():
            # Экранируем HTML
            safe_para = html.escape(para.strip())
            # Заменяем одиночные переносы строк на <br>
            safe_para = safe_para.replace('\n', '<br>')
            formatted_paragraphs.append(f'<p>{safe_para}</p>')

    if formatted_paragraphs:
        return '\n'.join(formatted_paragraphs)
    else:
        # Если нет абзацев
        safe_text = html.escape(text.replace('\n', '<br>'))
        return f'<p>{safe_text}</p>'


def _find_all_occurrences_with_context(text: str, search_text: str, name: str,
                                       category: str, class_name: str) -> List[Dict]:
    """
    Находит все вхождения текста с учетом контекста
    УЛУЧШЕНО: Разрешает дефисы в составных словах
    """
    occurrences = []
    if not search_text or not text:
        return occurrences

    search_lower = search_text.lower()
    text_lower = text.lower()
    search_len = len(search_text)

    pos = 0
    while True:
        found_pos = text_lower.find(search_lower, pos)
        if found_pos == -1:
            break

        # Проверяем границы слова с поддержкой дефисов
        is_valid = True

        # Проверяем начало
        if found_pos > 0:
            prev_char = text[found_pos - 1]
            # Допускаем дефис, пробел, скобку перед словом
            if prev_char.isalnum() and prev_char not in "-'(\"":
                is_valid = False

        # Проверяем конец
        end_pos = found_pos + search_len
        if end_pos < len(text):
            next_char = text[end_pos]
            # Допускаем: дефис, апостроф, пробел, пунктуация
            if next_char.isalnum():
                # Разрешаем 's для притяжательного падежа
                if next_char == 's' and end_pos + 1 < len(text) and text[end_pos + 1] in " ,.!?;:'\")":
                    # "devil's" - допустимо
                    pass
                # Разрешаем дефис после слова
                elif next_char == '-':
                    # "devil-worshipping" - допустимо
                    pass
                else:
                    is_valid = False

        # Проверяем, что не внутри HTML тега
        if is_valid and not _is_inside_html_tag(text, found_pos):
            occurrences.append({
                'start': found_pos,
                'end': end_pos,
                'name': name,
                'category': category,
                'class_name': class_name,
                'text': text[found_pos:end_pos]
            })

        pos = found_pos + 1

    return occurrences


def _is_inside_html_tag(text: str, position: int) -> bool:
    """
    Проверяет, находится ли позиция внутри HTML тега
    """
    if position < 0 or position >= len(text):
        return False

    # Находим последний открывающий тег до этой позиции
    last_lt = text.rfind('<', 0, position)
    if last_lt == -1:
        return False

    # Находим последний закрывающий тег до этой позиции
    last_gt = text.rfind('>', 0, position)

    # Если есть < без соответствующего >, значит мы внутри тега
    return last_lt > last_gt


def find_all_occurrences(text: str, search_text: str, name: str, category: str) -> List[Dict]:
    """
    Находит все вхождения текста в строке
    """
    occurrences = []
    if not search_text or not text:
        return occurrences

    search_lower = search_text.lower()
    text_lower = text.lower()
    search_len = len(search_text)

    pos = 0
    while True:
        # Ищем вхождение
        found_pos = text_lower.find(search_lower, pos)
        if found_pos == -1:
            break

        # Проверяем границы слова
        if found_pos > 0 and text[found_pos - 1].isalnum():
            pos = found_pos + 1
            continue

        if found_pos + search_len < len(text) and text[found_pos + search_len].isalnum():
            pos = found_pos + 1
            continue

        # Проверяем, что не внутри HTML тега
        if not is_inside_html_tag(text, found_pos):
            occurrences.append({
                'start': found_pos,
                'end': found_pos + search_len,
                'name': name,
                'category': category,
                'text': text[found_pos:found_pos + search_len]
            })

        pos = found_pos + search_len

    return occurrences


def remove_duplicate_matches(matches: List[Dict]) -> List[Dict]:
    """
    Удаляет дублирующиеся и пересекающиеся совпадения
    """
    if not matches:
        return []

    # Сортируем по начальной позиции
    matches.sort(key=lambda x: x['start'])

    result = []
    last_end = -1

    for match in matches:
        # Если нет пересечения с предыдущим, добавляем
        if match['start'] >= last_end:
            result.append(match)
            last_end = match['end']
        else:
            # Есть пересечение, выбираем более длинное совпадение
            last_match = result[-1]
            current_length = match['end'] - match['start']
            last_length = last_match['end'] - last_match['start']

            if current_length > last_length:
                result[-1] = match
                last_end = match['end']

    return result


def get_html_position_from_text(html_text: str, text_lower: str, search_text: str, position_in_clean: int) -> Dict:
    """
    Находит позицию текста в HTML с учетом тегов
    """
    search_lower = search_text.lower()

    # Ищем совпадение в нижнем регистре HTML текста
    html_lower = html_text.lower()

    # Ищем все возможные позиции вхождения
    start_pos = 0
    while True:
        pos = html_lower.find(search_lower, start_pos)
        if pos == -1:
            break

        # Проверяем, что найденный текст не внутри HTML тега
        before_text = html_text[:pos]
        last_tag_start = before_text.rfind('<')
        last_tag_end = before_text.rfind('>')

        # Если мы не внутри тега, возвращаем позицию
        if last_tag_start <= last_tag_end or last_tag_start == -1:
            return {
                'start': pos,
                'end': pos + len(search_lower),
                'text': html_text[pos:pos + len(search_lower)]
            }

        start_pos = pos + 1

    # Если не нашли, пытаемся найти по смещению
    # Это менее точный метод, но может помочь
    if position_in_clean > 0:
        # Преобразуем позицию в чистом тексте в позицию в HTML
        clean_text = re.sub(r'<[^>]+>', '', html_text)
        if position_in_clean < len(clean_text):
            # Находим соответствующий текст в чистом виде
            clean_match = clean_text[position_in_clean:position_in_clean + len(search_text)]

            # Ищем этот текст в HTML
            html_pos = html_text.find(clean_match)
            if html_pos != -1:
                return {
                    'start': html_pos,
                    'end': html_pos + len(clean_match),
                    'text': clean_match
                }

    return {'start': -1, 'end': -1, 'text': ''}


def find_all_matches_in_html_text(html_text: str, results: Dict) -> List[Dict]:
    """
    Находит все вхождения элементов в HTML тексте
    Сохраняет HTML теги
    """
    # Получаем чистый текст только для поиска
    clean_text = re.sub(r'<[^>]+>', '', html_text)
    clean_text_lower = clean_text.lower()

    matches = []
    element_names = {}

    # Собираем все имена элементов
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    for category in categories:
        if category in results:
            items = results[category].get('items', [])
            for item in items:
                element_names[item['name'].lower()] = {
                    'category': category,
                    'name': item['name'],
                    'original_name': item['name']
                }

    # Находим совпадения в чистых тексте
    for element_lower, element_info in element_names.items():
        # Для отдельных слов используем границы слов
        import re
        pattern = rf'\b{re.escape(element_lower)}\b'

        for match in re.finditer(pattern, clean_text_lower):
            # Находим соответствующий текст в HTML
            clean_match_text = clean_text[match.start():match.end()]

            # Ищем этот текст в оригинальном HTML
            html_pos = html_text.find(clean_match_text)

            if html_pos != -1:
                # Проверяем, что не внутри HTML тега
                before_text = html_text[:html_pos]
                last_tag_start = before_text.rfind('<')
                last_tag_end = before_text.rfind('>')

                if last_tag_start <= last_tag_end or last_tag_start == -1:
                    matches.append({
                        'start': html_pos,
                        'end': html_pos + len(clean_match_text),
                        'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                        'name': element_info['name'],
                        'text': clean_match_text,
                        'category': element_info['category']
                    })

    return matches


def get_html_position_from_plain_position(html_text: str, plain_text: str, plain_pos: int) -> int:
    """
    Конвертирует позицию в plain text в позицию в HTML тексте
    Считает символы, игнорируя HTML теги
    """
    if plain_pos < 0 or plain_pos > len(plain_text):
        return -1

    in_tag = False
    in_entity = False
    plain_chars_counted = 0

    for i, char in enumerate(html_text):
        if char == '<':
            in_tag = True
        elif char == '>':
            in_tag = False
        elif char == '&':
            in_entity = True
        elif char == ';' and in_entity:
            in_entity = False
        elif not in_tag and not in_entity:
            # Это текстовый символ (не в теге и не в HTML entity)
            if plain_chars_counted == plain_pos:
                return i
            # Учитываем только реальные символы, игнорируем пробелы и переносы строк для точности
            if char.strip() or char == ' ':
                plain_chars_counted += 1

    # Если не нашли, возвращаем последнюю позицию
    return len(html_text) if plain_chars_counted == plain_pos else -1


def find_all_matches_in_text_plain(plain_text: str, results: Dict) -> List[Dict]:
    """
    Простой поиск совпадений в plain text
    УЛУЧШЕНО: находит слова внутри составных слов через дефис
    """
    matches = []
    text_lower = plain_text.lower()

    # Словарь соответствий имен элементов
    element_names = {}

    # Собираем все имена элементов
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
    for category in categories:
        if category in results:
            items = results[category].get('items', [])
            for item in items:
                element_names[item['name'].lower()] = {
                    'category': category,
                    'name': item['name']
                }

    # Находим все вхождения каждого элемента в тексте
    for element_lower, element_info in element_names.items():
        start_pos = 0

        # Используем улучшенный поиск с поддержкой дефисов
        while True:
            pos = text_lower.find(element_lower, start_pos)
            if pos == -1:
                break

            # Проверяем границы слова с поддержкой дефисов
            is_valid = True

            # Проверяем начало
            if pos > 0:
                prev_char = plain_text[pos - 1]
                if prev_char.isalnum() and prev_char != '-':
                    is_valid = False

            # Проверяем конец
            end_pos = pos + len(element_lower)
            if end_pos < len(plain_text):
                next_char = plain_text[end_pos]
                # Допускаем дефис, апостроф, или конец слова
                if next_char.isalnum() and next_char not in "s'-":
                    is_valid = False
                # Разрешаем 's' только если дальше не буква
                elif next_char == 's':
                    if end_pos + 1 < len(plain_text) and plain_text[end_pos + 1].isalnum():
                        is_valid = False

            if is_valid:
                matches.append({
                    'start': pos,
                    'end': end_pos,
                    'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                    'name': element_info['name'],
                    'text': plain_text[pos:end_pos],
                    'category': element_info['category']
                })

            start_pos = pos + 1

    return matches


@login_required
@user_passes_test(is_staff_or_superuser)
def normalize_keyword(request: HttpRequest):
    """Возвращает нормализованную (словарную) форму для переданного слова."""
    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid request method'}, status=405)

    try:
        import json
        data = json.loads(request.body)
        word = data.get('word', '').strip()

        if not word:
            return JsonResponse({'success': False, 'message': 'Please enter a word to normalize'})

        # Используем логику из команды normalize_keywords для получения базовой формы
        # Создаем экземпляр команды и вызываем её метод _get_base_form
        normalizer = NormalizeCommand()
        normalizer._init_nltk()  # Убедимся, что WordNet доступен

        # Короткие слова не нормализуем
        if normalizer._is_short_word(word):
            base_form = word
        # Игровые термины не нормализуем
        elif normalizer._is_gaming_term(word):
            base_form = word
        else:
            base_form = normalizer._get_base_form(word)

        return JsonResponse({
            'success': True,
            'original': word,
            'normalized': base_form,
            'message': f'Normalized form: "{base_form}"'
        })

    except Exception as e:
        print(f"✗ ОШИБКА нормализации слова: {str(e)}")
        return JsonResponse({
            'success': False,
            'message': f'Error normalizing word: {str(e)}'
        })


@login_required
@user_passes_test(is_staff_or_superuser)
def clear_analysis_results(request: HttpRequest, game_id: int):
    """Очищает несохраненные результаты анализа и подсветку"""
    game = get_object_or_404(Game, pk=game_id)

    # Получаем текущую вкладку из запроса
    active_tab = request.GET.get('tab', 'summary')

    # Ключи, которые нужно удалить из сессии
    session_keys_to_remove = [
        f'unsaved_results_{game.id}',
        f'highlight_enabled_{game.id}',
        f'analysis_data_{game.id}',
    ]

    # Удаляем данные из сессии
    removed_count = 0
    for key in session_keys_to_remove:
        if key in request.session:
            del request.session[key]
            removed_count += 1

    # Также очищаем все сохраненные подсветки для всех вкладок
    game_descriptions = {
        'summary': game.summary or '',
        'storyline': game.storyline or '',
        'rawg': game.rawg_description or '',
        'wiki': game.wiki_description or '',
    }

    # Сохраняем изменения в сессии
    if removed_count > 0:
        request.session.modified = True
        messages.success(request, f'✅ Успешно очищено {removed_count} сохранённых результатов анализа.')
    else:
        messages.info(request, 'ℹ️ Не было сохранённых результатов для очистки.')

    # Проверяем, это AJAX запрос или обычный
    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        # Для AJAX возвращаем JSON ответ
        return JsonResponse({
            'success': True,
            'message': f'Очищено {removed_count} результатов',
            'redirect_url': reverse('analyze_game', args=[game.id]) + f'?tab={active_tab}&cleared=1'
        })

    # Для обычных запросов - редирект с сохранением вкладки
    redirect_url = reverse('analyze_game', args=[game.id]) + f'?tab={active_tab}&cleared=1'
    return redirect(redirect_url)
