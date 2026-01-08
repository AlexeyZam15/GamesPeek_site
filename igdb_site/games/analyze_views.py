# games/analyze_views.py
"""
Views для анализа описаний игр - ТОЛЬКО комбинированный режим
"""
from django.utils.safestring import mark_safe
import json
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, JsonResponse
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.utils import timezone
from django.urls import reverse

from typing import Dict, Any, List, Tuple
from .models import Game, Keyword, KeywordCategory
from .analyze.game_analyzer_api import GameAnalyzerAPI
from django.db import models
from django.utils.html import escape, strip_tags, mark_safe
import html
import re
import html
from typing import Dict, Any, List, Tuple
from django.utils.html import escape, strip_tags, mark_safe


# ===== УТИЛИТЫ ДЛЯ ПРОВЕРКИ ПРАВ =====
def is_staff_or_superuser(user):
    """Проверяет, является ли пользователь staff или superuser"""
    return user.is_authenticated and (user.is_staff or user.is_superuser)


def analyze_single_game(request: HttpRequest, game_id: int):
    """Анализ одной игры - ТОЛЬКО комбинированный режим"""

    # Инициализация
    game, original_descriptions, active_tab = _initialize_analysis_context(request, game_id)

    # Обработка POST запросов
    if request.method == 'POST':
        return _handle_post_request(request, game, original_descriptions, active_tab)

    # Подготовка контекста для GET запроса
    context = _prepare_get_context(request, game, original_descriptions, active_tab)

    return render(request, 'games/analyze.html', context)


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


def _handle_post_request(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка POST запросов"""

    # Добавление ключевого слова
    if 'add_keyword' in request.POST:
        return _handle_add_keyword(request, game, original_descriptions, active_tab)

    # Анализ текста
    elif 'analyze' in request.POST:
        return _handle_analyze_request(request, game, original_descriptions, active_tab)

    # Сохранение результатов
    elif 'save_results' in request.POST:
        return _handle_save_results(request, game, original_descriptions, active_tab)

    # По умолчанию - редирект на ту же вкладку
    return _redirect_to_tab(game.id, active_tab)


def _handle_add_keyword(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str):
    """Обработка добавления ключевого слова"""
    keyword_name = request.POST.get('new_keyword', '').strip()
    auto_analyze = request.POST.get('auto_analyze', 'false') == 'true'

    if not keyword_name:
        messages.error(request, '❌ Please enter a keyword')
        return _redirect_to_tab(game.id, active_tab)

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

        # Обновляем данные игры
        game.refresh_from_db()
        messages.success(request, f'✅ Keyword "{keyword_name}" added to game!')

        # Автоматически анализируем после добавления
        if auto_analyze and active_tab in original_descriptions:
            text = original_descriptions[active_tab]
            if text:
                try:
                    analyzer = GameAnalyzerAPI(verbose=True)
                    analysis_result = analyzer.analyze_game_text_comprehensive(
                        text=text,
                        game_id=game.id,
                        existing_game=game,
                        exclude_existing=False
                    )

                    if analysis_result['success']:
                        _save_analysis_results(request, game.id, active_tab, text, analysis_result)
                        messages.info(request,
                                      '🔍 Текст автоматически проанализирован после добавления ключевого слова.')
                except Exception as e:
                    messages.warning(request, f'⚠️ Ключевое слово добавлено, но возникла ошибка при анализе: {str(e)}')

        # Редирект с параметрами
        redirect_url = reverse('analyze_game', args=[game.id])
        params = []
        if active_tab and active_tab != 'summary':
            params.append(f'tab={active_tab}')
        params.append('keyword_added=1')
        params.append('auto_analyze=1')

        if params:
            redirect_url += '?' + '&'.join(params)

        return redirect(redirect_url)

    except Exception as e:
        messages.error(request, f'❌ Error adding keyword: {str(e)}')
        return _redirect_to_tab(game.id, active_tab)


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

            # Редирект с флагом сохранения
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

    return _redirect_to_tab(game.id, active_tab)


def _save_analysis_results(request: HttpRequest, game_id: int, tab: str, original_text: str, analysis_result: Dict):
    """Сохранение результатов анализа в сессии
    ИСПРАВЛЕНИЕ: Безопасное сохранение HTML
    """
    # Форматируем оригинальный текст для подсветки
    formatted_text = format_text_with_html(original_text)

    # Создаем подсвеченный текст на основе форматированного
    highlighted_text = highlight_matches_in_text_combined(
        text=formatted_text,
        analysis_result=analysis_result
    )

    # Извлекаем найденные элементы
    found_items = extract_found_items_combined(analysis_result, Game.objects.get(id=game_id))

    # Получаем или создаем структуру для сохранения
    unsaved_results = request.session.get(f'unsaved_results_{game_id}', {
        'highlighted_text': {},
        'found_items': {},
        'analysis_data': {},
        'save_data': {}
    })

    # ИСПРАВЛЕНИЕ: Сохраняем как обычный текст
    unsaved_results['highlighted_text'][tab] = highlighted_text  # Уже HTML

    unsaved_results['found_items'][tab] = found_items

    # Сохраняем полные данные анализа
    unsaved_results['analysis_data'][tab] = {
        'text_source': tab,
        'results': analysis_result['results'],
        'summary': analysis_result['summary'],
        'pattern_info': analysis_result.get('pattern_info', {}),
        'total_matches': analysis_result.get('total_matches', 0),
        'original_text': original_text,
        'formatted_text': formatted_text  # Сохраняем как обычный текст
    }

    # Подготавливаем данные для сохранения (только новые элементы)
    save_data = {
        'text_source': tab,
        'results': {},
        'found_count': 0
    }

    game = Game.objects.get(id=game_id)
    total_new = 0
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

    for category in categories:
        if category in analysis_result['results']:
            all_items = analysis_result['results'][category].get('items', [])

            # Получаем существующие ID
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

            # Фильтруем новые элементы
            category_new_items = [item for item in all_items if item['id'] not in existing_ids]
            if category_new_items:
                save_data['results'][category] = {'items': category_new_items}
                total_new += len(category_new_items)

    save_data['found_count'] = total_new
    unsaved_results['save_data'] = save_data

    # Сохраняем в сессии
    request.session[f'unsaved_results_{game_id}'] = unsaved_results
    request.session.modified = True


from django.utils.safestring import mark_safe


def _prepare_get_context(request: HttpRequest, game: Game, original_descriptions: Dict, active_tab: str) -> Dict:
    """Подготовка контекста для GET запроса
    ИСПРАВЛЕНИЕ: Упрощенная логика с сохранением абзацной структуры
    """

    # Проверяем флаг auto_analyze в URL
    auto_analyze_flag = request.GET.get('auto_analyze', '0') == '1'
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


def _redirect_to_tab(game_id: int, tab: str):
    """Редирект на указанную вкладку"""
    redirect_url = reverse('analyze_game', args=[game_id])
    if tab and tab != 'summary':
        redirect_url += f'?tab={tab}'
    return redirect(redirect_url)


def highlight_matches_in_text_combined(text: str, analysis_result: Dict) -> str:
    """
    Безопасная подсветка совпадений в тексте
    """
    if not text or not analysis_result.get('pattern_info'):
        return text if text else ""

    # Текст уже должен быть в HTML формате (с <p> тегами)
    working_text = text

    # Создаем список всех совпадений без пересечений
    all_matches = []
    categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']

    # Проходим по всем совпадениям и собираем их
    for category in categories:
        category_matches = analysis_result.get('pattern_info', {}).get(category, [])
        for match in category_matches:
            if match.get('status') == 'found':
                matched_text = match.get('matched_text', '')
                match_name = match.get('name', '')

                if not matched_text or not match_name:
                    continue

                # Ищем все вхождения этого текста
                matches_in_text = find_all_occurrences(working_text, matched_text, match_name, category)
                all_matches.extend(matches_in_text)

    # Если нет совпадений, возвращаем исходный текст
    if not all_matches:
        return mark_safe(working_text)

    # Удаляем дубликаты и пересечения
    unique_matches = remove_duplicate_matches(all_matches)

    # Сортируем от конца к началу для корректной замены
    unique_matches.sort(key=lambda x: x['start'], reverse=True)

    # Применяем подсветку
    highlighted_text = working_text

    for match in unique_matches:
        try:
            # Проверяем границы
            if match['start'] < 0 or match['end'] > len(highlighted_text) or match['start'] >= match['end']:
                continue

            # Получаем оригинальный текст
            original_text = highlighted_text[match['start']:match['end']]

            # Пропускаем если текст уже внутри HTML тега
            if is_inside_html_tag(highlighted_text, match['start']):
                continue

            # Экранируем специальные символы в имени
            from django.utils.html import escape
            safe_name = escape(match['name'])

            # Определяем класс подсветки
            type_name = match['category'][:-1] if match['category'] != 'keywords' else 'keyword'
            span_class = f"highlight-{type_name}"
            span_title = f"{type_name.title()}: {safe_name}"

            # Создаем span
            highlighted_span = (
                f'<span class="{span_class}" '
                f'data-element-name="{safe_name}" '
                f'data-category="{match["category"]}" '
                f'title="{span_title}">'
                f'{original_text}'
                f'</span>'
            )

            # Заменяем текст
            highlighted_text = (
                    highlighted_text[:match['start']] +
                    highlighted_span +
                    highlighted_text[match['end']:]
            )

        except Exception as e:
            print(f"Ошибка при подсветке '{match.get('name', 'unknown')}': {e}")
            continue

    return highlighted_text  # Убрал mark_safe, так как он будет применен позже


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


def is_inside_html_tag(text: str, position: int) -> bool:
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


def format_text_with_html(text: str) -> str:
    """
    Преобразует обычный текст в безопасный HTML с сохранением абзацной структуры
    Если текст уже содержит HTML теги - использует его как есть (с безопасной обработкой)
    """
    if not text:
        return ""

    # Проверяем, содержит ли текст HTML теги
    has_html_tags = '<' in text and '>' in text

    if has_html_tags:
        # Если текст уже содержит HTML, применяем безопасную обработку
        # но сохраняем существующие теги
        from django.utils.html import mark_safe
        # mark_safe уже применен в вызывающем коде, поэтому просто возвращаем текст
        return text

    # Если нет HTML тегов, форматируем как обычный текст
    # Разделяем на абзацы по двойным переносам строк
    paragraphs = text.split('\n\n')

    formatted_paragraphs = []

    for para in paragraphs:
        if para.strip():
            # Экранируем HTML внутри абзаца
            from django.utils.html import escape
            safe_para = escape(para.strip())
            # Заменяем одиночные переносы строк на <br>
            safe_para = safe_para.replace('\n', '<br>')
            formatted_paragraphs.append(f'<p>{safe_para}</p>')

    if formatted_paragraphs:
        return '\n'.join(formatted_paragraphs)
    else:
        # Если нет абзацев (например, весь текст в одной строке)
        from django.utils.html import escape
        safe_text = escape(text.replace('\n', '<br>'))
        return f'<p>{safe_text}</p>'


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
