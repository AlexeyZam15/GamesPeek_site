# games/analyze_views.py
"""
Views для анализа описаний игр - ТОЛЬКО комбинированный режим
"""

import json
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, JsonResponse
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.utils import timezone
from django.urls import reverse
from django.utils.html import escape, strip_tags

from typing import Dict, Any, List
from .models import Game, Keyword, KeywordCategory
from .analyze.game_analyzer_api import GameAnalyzerAPI
from django.db import models


# ===== УТИЛИТЫ ДЛЯ ПРОВЕРКИ ПРАВ =====
def is_staff_or_superuser(user):
    """Проверяет, является ли пользователь staff или superuser"""
    return user.is_authenticated and (user.is_staff or user.is_superuser)


@login_required
@user_passes_test(is_staff_or_superuser)
def analyze_single_game(request: HttpRequest, game_id: int):
    """Анализ одной игры - ТОЛЬКО комбинированный режим с подсветкой ВСЕХ вхождений"""
    game = get_object_or_404(Game, pk=game_id)

    # Получаем ВСЕ текстовые описания
    descriptions = {
        'summary': game.summary or '',
        'storyline': game.storyline or '',
        'rawg': game.rawg_description or '',
        'wiki': game.wiki_description or '',
    }

    # Фильтруем непустые описания
    available_descriptions = {k: v for k, v in descriptions.items() if v.strip()}

    # Определяем активную вкладку
    active_tab = request.GET.get('tab', 'summary')
    if active_tab not in available_descriptions:
        active_tab = next(iter(available_descriptions.keys()), 'summary')

    # ВСЕГДА комбинированный режим
    analyze_mode = 'combined'

    # Переменные для результатов
    highlighted_text = ''
    found_items = {}

    # Флаг для включения/выключения подсветки
    highlight_enabled = request.session.get(f'highlight_enabled_{game.id}', True)

    # Если пришел POST запрос на добавление ключевого слова
    if request.method == 'POST' and 'add_keyword' in request.POST:
        keyword_name = request.POST.get('new_keyword', '').strip()

        if keyword_name:
            try:
                # Создаем или получаем ключевое слово
                keyword = Keyword.objects.filter(name__iexact=keyword_name).first()

                if not keyword:
                    # Создаем новое ключевое слово
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

                # Обновляем данные игры
                game.refresh_from_db()
                messages.success(request, f'✅ Keyword "{keyword_name}" added to game!')

                # ВАЖНО: сохраняем активную вкладку в редиректе
                redirect_url = reverse('analyze_game', args=[game.id])

                # Добавляем параметры
                params = []
                if active_tab and active_tab != 'summary':
                    params.append(f'tab={active_tab}')
                params.append('keyword_added=1')

                if params:
                    redirect_url += '?' + '&'.join(params)

                return redirect(redirect_url)

            except Exception as e:
                messages.error(request, f'❌ Error adding keyword: {str(e)}')

        # Если keyword_name пустой, остаемся на той же вкладке
        redirect_url = reverse('analyze_game', args=[game.id])
        if active_tab and active_tab != 'summary':
            redirect_url += f'?tab={active_tab}'
        return redirect(redirect_url)

    # Если пришел POST запрос на анализ
    elif request.method == 'POST' and 'analyze' in request.POST:
        # Получаем параметры
        analyze_tab = request.POST.get('analyze_tab', active_tab)

        # Проверяем, был ли отправлен переключатель подсветки
        highlight_toggle = request.POST.get('highlight_toggle')
        if highlight_toggle is not None:
            highlight_enabled = (highlight_toggle == 'on')
            request.session[f'highlight_enabled_{game.id}'] = highlight_enabled

        if analyze_tab in available_descriptions:
            text = available_descriptions[analyze_tab]

            if not text:
                messages.error(request, '❌ No text to analyze.')
            else:
                try:
                    analyzer = GameAnalyzerAPI(verbose=True)

                    # ВСЕГДА используем комплексный анализ с поиском ВСЕХ вхождений
                    analysis_result = analyzer.analyze_game_text_comprehensive(
                        text=text,
                        game_id=game.id,
                        existing_game=game,
                        exclude_existing=False  # Показываем ВСЕ вхождения
                    )

                    if analysis_result['success']:
                        # Подсвечиваем текст с учетом ВСЕХ вхождений
                        highlighted_text = highlight_matches_in_text_combined(
                            text=text,
                            analysis_result=analysis_result
                        )

                        # Сохраняем результаты для отображения
                        found_items = extract_found_items_combined(analysis_result, game)

                        # Сохраняем результаты в сессии
                        unsaved_results = request.session.get(f'unsaved_results_{game.id}', {
                            'highlighted_text': {},
                            'found_items': {},
                            'analysis_data': {},
                            'save_data': {}
                        })

                        unsaved_results['highlighted_text'][analyze_tab] = highlighted_text
                        unsaved_results['found_items'][analyze_tab] = found_items
                        unsaved_results['analysis_data'][analyze_tab] = {
                            'text_source': analyze_tab,
                            'results': analysis_result['results'],
                            'summary': analysis_result['summary'],
                            'all_results': analysis_result['results'],
                            'pattern_info': analysis_result.get('pattern_info', {}),
                            'total_matches': analysis_result.get('total_matches', 0)
                        }

                        # Для сохранения: фильтруем только новые элементы
                        save_data = {
                            'text_source': analyze_tab,
                            'results': {},
                            'found_count': 0
                        }

                        total_new = 0
                        categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

                        for category in categories:
                            if category in analysis_result['results']:
                                all_items = analysis_result['results'][category].get('items', [])

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

                                category_new_items = [item for item in all_items if item['id'] not in existing_ids]
                                if category_new_items:
                                    save_data['results'][category] = {'items': category_new_items}
                                    total_new += len(category_new_items)

                        save_data['found_count'] = total_new
                        unsaved_results['save_data'] = save_data
                        request.session[f'unsaved_results_{game.id}'] = unsaved_results
                        request.session.modified = True

                        # Сообщение о найденных элементах
                        total_found = analysis_result['summary'].get('found_count', 0)
                        total_matches = analysis_result.get('total_matches', 0)

                        if total_found > 0:
                            messages.success(request,
                                             f'🔍 Found {total_found} elements with {total_matches} total matches. '
                                             f'All matches highlighted. Click "Save Results" to save new elements to database.'
                                             )
                        else:
                            messages.info(request,
                                          'ℹ️ No new elements found in text. Existing elements are highlighted.')

                        # Переключаемся на проанализированную вкладку
                        active_tab = analyze_tab
                    else:
                        messages.error(request, f'❌ Analysis error: {analysis_result.get("error", "Unknown error")}')

                except Exception as e:
                    messages.error(request, f'❌ Error during analysis: {str(e)}')

        # После анализа остаемся на той же вкладке
        redirect_url = reverse('analyze_game', args=[game.id])
        if active_tab and active_tab != 'summary':
            redirect_url += f'?tab={active_tab}'
        return redirect(redirect_url)

    # Если пришел POST запрос на сохранение
    elif request.method == 'POST' and 'save_results' in request.POST:
        # Получаем сохраненные данные из сессии
        unsaved_results = request.session.get(f'unsaved_results_{game.id}', {})
        save_data = unsaved_results.get('save_data', {})

        if not save_data:
            messages.error(request, '❌ No analysis data to save. Perform analysis first.')
        else:
            try:
                analyzer = GameAnalyzerAPI(verbose=True)

                # При сохранении применяем данные к базе данных
                update_result = analyzer.update_game_with_combined_results(
                    game_id=game.id,
                    results=save_data['results']
                )

                if update_result['success']:
                    game.last_analyzed_date = timezone.now()
                    game.save()

                    # Обновляем данные игры
                    game.refresh_from_db()
                    descriptions = {
                        'summary': game.summary or '',
                        'storyline': game.storyline or '',
                        'rawg': game.rawg_description or '',
                        'wiki': game.wiki_description or '',
                    }
                    available_descriptions = {k: v for k, v in descriptions.items() if v.strip()}

                    # Очищаем сохраненные результаты из сессии
                    if f'unsaved_results_{game.id}' in request.session:
                        del request.session[f'unsaved_results_{game.id}']
                        request.session.modified = True

                    added_count = save_data.get('found_count', 0)
                    if added_count > 0:
                        messages.success(request,
                                         f'✅ Successfully added {added_count} new elements to the game!'
                                         )
                    else:
                        messages.info(request, 'ℹ️ No new elements to add (all found elements already exist in game).')

                    # Сохраняем активную вкладку в редиректе
                    redirect_url = reverse('analyze_game', args=[game.id])
                    if active_tab and active_tab != 'summary':
                        redirect_url += f'?tab={active_tab}'
                    redirect_url += '&saved=1'

                    return redirect(redirect_url)
                else:
                    messages.error(request,
                                   f'❌ Failed to save results: {update_result.get("error", "Unknown error")}')

            except Exception as e:
                messages.error(request, f'❌ Error saving results: {str(e)}')

        # Если ошибка сохранения, остаемся на той же вкладке
        redirect_url = reverse('analyze_game', args=[game.id])
        if active_tab and active_tab != 'summary':
            redirect_url += f'?tab={active_tab}'
        return redirect(redirect_url)

    # Если есть сохраненные результаты в сессии для текущей вкладки, используем их
    unsaved_results = request.session.get(f'unsaved_results_{game.id}', {})
    unsaved_highlighted_text = unsaved_results.get('highlighted_text', {}).get(active_tab, '')
    unsaved_found_items = unsaved_results.get('found_items', {}).get(active_tab, {})

    if unsaved_highlighted_text and highlight_enabled:
        highlighted_text = unsaved_highlighted_text
        found_items = unsaved_found_items

    # Подготавливаем контекст для отображения вкладок
    description_tabs = []
    tab_labels = {
        'summary': 'Описание',
        'storyline': 'Сюжет',
        'rawg': 'RAWG',
        'wiki': 'Wikipedia'
    }

    for tab_key, tab_label in tab_labels.items():
        if tab_key in available_descriptions:
            text = available_descriptions[tab_key]
            is_active = (tab_key == active_tab)

            # Получаем подсвеченный текст если это активная вкладка и есть сохраненные результаты
            display_text = text
            is_highlighted = False

            if is_active and highlighted_text and highlight_enabled:
                display_text = highlighted_text
                is_highlighted = True
            elif is_active and not highlight_enabled and text:
                # Если подсветка выключена, показываем обычный текст
                display_text = text
                is_highlighted = False

            description_tabs.append({
                'key': tab_key,
                'label': tab_label,
                'length': len(text),
                'is_active': is_active,
                'text': display_text,
                'is_highlighted': is_highlighted
            })

    # Текущие критерии игры
    current_genres = list(game.genres.all())
    current_themes = list(game.themes.all())
    current_perspectives = list(game.player_perspectives.all())
    current_game_modes = list(game.game_modes.all())
    current_keywords = list(game.keywords.all())

    # Проверяем, есть ли несохраненные результаты
    has_unsaved_results = bool(request.session.get(f'unsaved_results_{game.id}', {}).get('save_data'))

    context = {
        'game': game,
        'description_tabs': description_tabs,
        'current_genres': current_genres,
        'current_themes': current_themes,
        'current_perspectives': current_perspectives,
        'current_game_modes': current_game_modes,
        'current_keywords': current_keywords,
        'analyze_mode': analyze_mode,
        'found_items': found_items,
        'has_unsaved_results': has_unsaved_results,
        'highlight_enabled': highlight_enabled,
        'active_tab': active_tab,
    }

    return render(request, 'games/analyze.html', context)


def highlight_matches_in_text_combined(text: str, analysis_result: Dict) -> str:
    """
    Подсвечивает ВСЕ найденные элементы в HTML тексте
    Сохраняет оригинальное форматирование, теги <mark> вставляются внутрь HTML
    """
    if not text or not analysis_result.get('pattern_info'):
        return text

    # Работаем с HTML как есть
    working_text = text

    # Получаем все совпадения с позициями
    matches = []

    # Добавляем совпадения из всех категорий
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

    for category in categories:
        category_matches = analysis_result.get('pattern_info', {}).get(category, [])
        for match in category_matches:
            if match.get('status') == 'found':
                # Позиции из анализатора соответствуют оригинальному тексту
                matches.append({
                    'start': match.get('position', 0),
                    'end': match.get('position', 0) + len(match.get('matched_text', '')),
                    'type': category[:-1] if category != 'keywords' else 'keyword',
                    'name': match.get('name'),
                    'text': match.get('matched_text'),
                    'category': category
                })

    # Если нет совпадений в pattern_info, ищем вручную в оригинальном тексте
    if not matches and analysis_result.get('results'):
        matches = find_all_matches_in_html_text(working_text, analysis_result['results'])

    # Сортируем по позиции (от конца к началу) - ВАЖНО для корректной вставки
    matches.sort(key=lambda x: x['start'], reverse=True)

    # Применяем подсветку, вставляя <mark> теги в HTML
    highlighted_text = working_text

    for match in matches:
        # Проверяем, что позиции валидны
        if match['start'] < 0 or match['end'] > len(highlighted_text) or match['start'] >= match['end']:
            continue

        try:
            before = highlighted_text[:match['start']]
            matched = highlighted_text[match['start']:match['end']]
            after = highlighted_text[match['end']:]

            # Вставляем тег <mark> с атрибутами
            highlighted = (
                f'<mark class="highlight-{match["type"]}" '
                f'data-element-name="{match["name"]}" '
                f'data-bs-toggle="tooltip" data-bs-title="{match["name"]}" '
                f'data-category="{match["category"]}">'
                f'{matched}'
                f'</mark>'
            )

            highlighted_text = before + highlighted + after
        except Exception as e:
            # Если ошибка - пропускаем этот match и продолжаем
            print(f"Error highlighting match '{match['name']}': {e}")
            continue

    return highlighted_text


def find_all_matches_in_html_text(html_text: str, results: Dict) -> List[Dict]:
    """
    Находит все вхождения элементов в HTML тексте
    Ищет в plain text версии, но возвращает позиции для HTML
    """
    from django.utils.html import strip_tags
    from django.utils.html import escape

    # Создаем plain text версию для поиска
    plain_text = strip_tags(html_text)
    plain_text_lower = plain_text.lower()

    matches = []

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

    # Находим все вхождения каждого элемента в plain text
    for element_lower, element_info in element_names.items():
        start_pos = 0

        if ' ' in element_lower:
            # Для фраз из нескольких слов
            while True:
                pos = plain_text_lower.find(element_lower, start_pos)
                if pos == -1:
                    break

                # Преобразуем позицию из plain text в позицию в HTML
                html_pos = get_html_position_from_plain_position(html_text, plain_text, pos)
                html_end_pos = get_html_position_from_plain_position(html_text, plain_text, pos + len(element_lower))

                if html_pos != -1 and html_end_pos != -1:
                    matches.append({
                        'start': html_pos,
                        'end': html_end_pos,
                        'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                        'name': element_info['name'],
                        'text': html_text[html_pos:html_end_pos],
                        'category': element_info['category']
                    })

                start_pos = pos + 1
        else:
            # Для отдельных слов (с учетом границ слов)
            import re
            pattern = rf'\b{re.escape(element_lower)}\b'

            for match in re.finditer(pattern, plain_text_lower):
                pos = match.start()
                html_pos = get_html_position_from_plain_position(html_text, plain_text, pos)
                html_end_pos = get_html_position_from_plain_position(html_text, plain_text, match.end())

                if html_pos != -1 and html_end_pos != -1:
                    matches.append({
                        'start': html_pos,
                        'end': html_end_pos,
                        'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                        'name': element_info['name'],
                        'text': html_text[html_pos:html_end_pos],
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
    plain_chars_counted = 0
    html_pos = 0

    for i, char in enumerate(html_text):
        if char == '<':
            in_tag = True
        elif char == '>':
            in_tag = False
        elif not in_tag and char != '\n' and char != '\r':
            # Это текстовый символ (не в теге)
            if plain_chars_counted == plain_pos:
                return i
            plain_chars_counted += 1

        html_pos = i

    # Если не нашли, возвращаем последнюю позицию
    return len(html_text) if plain_chars_counted == plain_pos else -1


def find_all_matches_in_text_plain(plain_text: str, results: Dict) -> List[Dict]:
    """
    Простой поиск совпадений в plain text
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

        if ' ' in element_lower:
            # Для фраз из нескольких слов
            while True:
                pos = text_lower.find(element_lower, start_pos)
                if pos == -1:
                    break

                matches.append({
                    'start': pos,
                    'end': pos + len(element_lower),
                    'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                    'name': element_info['name'],
                    'text': plain_text[pos:pos + len(element_lower)],
                    'category': element_info['category']
                })

                start_pos = pos + 1
        else:
            # Для отдельных слов
            import re
            pattern = rf'\b{re.escape(element_lower)}\b'

            for match in re.finditer(pattern, text_lower):
                matches.append({
                    'start': match.start(),
                    'end': match.end(),
                    'type': element_info['category'][:-1] if element_info['category'] != 'keywords' else 'keyword',
                    'name': element_info['name'],
                    'text': plain_text[match.start():match.end()],
                    'category': element_info['category']
                })

    return matches


def extract_found_items_combined(analysis_result: Dict, game=None) -> Dict:
    """
    Извлекает все найденные элементы для комбинированного режима
    """
    results = {}
    new_counts = {}

    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

    for category in categories:
        items = analysis_result.get('results', {}).get(category, {}).get('items', [])
        if items:
            found_items = []
            new_count = 0

            for item in items:
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
            new_counts[f'{category}_new_count'] = new_count
            results[f'{category}_new_count'] = new_count
        else:
            new_counts[f'{category}_new_count'] = 0
            results[f'{category}_new_count'] = 0

    # Добавляем общую статистику
    total_found = sum(len(results.get(cat, [])) for cat in categories)
    results['total_found'] = total_found

    # Добавляем информацию о количестве совпадений
    total_matches = analysis_result.get('total_matches', 0)
    results['total_matches'] = total_matches

    return results


@login_required
@user_passes_test(is_staff_or_superuser)
def clear_analysis_results(request: HttpRequest, game_id: int):
    """Очищает несохраненные результаты анализа"""
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
