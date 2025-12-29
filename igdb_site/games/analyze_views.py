# games/analyze_views.py
"""
Views для анализа описаний игр (с отдельной кнопкой сохранения)
"""

import json
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.utils import timezone
from django.urls import reverse
from django.utils.html import escape

from typing import Dict, Any, List, Optional, Tuple
from .models import Game
from .analyze.game_analyzer_api import GameAnalyzerAPI
from .analyze.utils import get_game_text


# ===== УТИЛИТЫ ДЛЯ ПРОВЕРКИ ПРАВ =====
def is_staff_or_superuser(user):
    """Проверяет, является ли пользователь staff или superuser"""
    return user.is_authenticated and (user.is_staff or user.is_superuser)


# ===== VIEW ДЛЯ АНАЛИЗА ОДНОЙ ИГРЫ =====


# games/analyze_views.py - исправленная версия

@login_required
@user_passes_test(is_staff_or_superuser)
def analyze_single_game(request: HttpRequest, game_id: int):
    """Анализ одной игры - показывает все описания с подсветкой"""
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

    # Режим анализа
    analyze_mode = request.GET.get('mode', 'criteria')  # 'criteria' или 'keywords'

    # Переменные для результатов
    highlighted_text = ''
    analysis_results = None
    found_items = {}

    # Флаг для включения/выключения подсветки
    highlight_enabled = request.session.get(f'highlight_enabled_{game.id}', True)

    # Если пришел POST запрос на анализ
    if request.method == 'POST' and 'analyze' in request.POST:
        # Получаем параметры
        analyze_tab = request.POST.get('analyze_tab', active_tab)
        analyze_mode = request.POST.get('analyze_mode', analyze_mode)

        # Проверяем, был ли отправлен переключатель подсветки
        highlight_toggle = request.POST.get('highlight_toggle')
        if highlight_toggle is not None:
            highlight_enabled = (highlight_toggle == 'on')
            request.session[f'highlight_enabled_{game.id}'] = highlight_enabled

        if analyze_tab in available_descriptions:
            text = available_descriptions[analyze_tab]

            if not text:
                messages.error(request, '❌ Нет текста для анализа.')
            else:
                try:
                    analyzer = GameAnalyzerAPI(verbose=True)

                    # Анализируем текст с exclude_existing=False чтобы найти ВСЕ элементы
                    analysis_result = analyzer.analyze_game_text(
                        text=text,
                        game_id=game.id,
                        analyze_keywords=(analyze_mode == 'keywords'),
                        existing_game=game,  # Передаем игру для информации
                        detailed_patterns=True,  # Для подсветки
                        exclude_existing=False  # НЕ исключать существующие
                    )

                    if analysis_result['success'] and analysis_result['has_results']:
                        # Подсвечиваем текст
                        highlighted_text = highlight_matches_in_text(
                            text=text,
                            analysis_result=analysis_result,
                            mode=analyze_mode
                        )

                        # Сохраняем результаты для отображения
                        analysis_results = analysis_result
                        found_items = extract_found_items(analysis_result, analyze_mode, game)

                        # Сохраняем результаты в сессии (НЕ в базе данных!)
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
                            'analyze_keywords': (analyze_mode == 'keywords'),
                            'results': analysis_result['results'],
                            'summary': analysis_result['summary'],
                            'all_results': analysis_result['results']
                        }

                        # Для сохранения: фильтруем только новые элементы
                        if analyze_mode == 'keywords':
                            # Для ключевых слов
                            all_items = analysis_result['results'].get('keywords', {}).get('items', [])
                            existing_keywords = set(game.keywords.values_list('id', flat=True))
                            new_items = [item for item in all_items if item['id'] not in existing_keywords]

                            save_data = {
                                'text_source': analyze_tab,
                                'analyze_keywords': True,
                                'results': {'keywords': {'items': new_items}},
                                'found_count': len(new_items),
                            }
                        else:
                            # Для критериев
                            new_items = {}
                            categories = ['genres', 'themes', 'perspectives', 'game_modes']
                            total_new = 0

                            for category in categories:
                                all_items = analysis_result['results'].get(category, {}).get('items', [])
                                if all_items:
                                    if category == 'genres':
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
                                        new_items[category] = {'items': category_new_items}
                                        total_new += len(category_new_items)

                            save_data = {
                                'text_source': analyze_tab,
                                'analyze_keywords': False,
                                'results': new_items,
                                'found_count': total_new,
                            }

                        unsaved_results['save_data'] = save_data
                        request.session[f'unsaved_results_{game.id}'] = unsaved_results
                        request.session.modified = True  # Важно!

                        # Сообщение о найденных элементах
                        total_found = analysis_result['summary'].get('found_count', 0)
                        messages.success(request,
                                         f'🔍 Найдено {total_found} элементов. '
                                         f'Текст подсвечен. Нажмите "Save Results" чтобы сохранить в базу данных.'
                                         )

                        # Переключаемся на проанализированную вкладку
                        active_tab = analyze_tab
                    else:
                        messages.info(request, 'ℹ️ Элементы не найдены в тексте.')

                except Exception as e:
                    messages.error(request, f'❌ Ошибка при анализе: {str(e)}')

    # Если пришел POST запрос на сохранение
    elif request.method == 'POST' and 'save_results' in request.POST:
        # Получаем сохраненные данные из сессии
        unsaved_results = request.session.get(f'unsaved_results_{game.id}', {})
        save_data = unsaved_results.get('save_data', {})

        if not save_data:
            messages.error(request, '❌ Нет данных анализа для сохранения. Сначала выполните анализ.')
        else:
            try:
                analyzer = GameAnalyzerAPI(verbose=True)

                # При сохранении применяем данные к базе данных
                update_result = analyzer.update_game_with_results(
                    game_id=game.id,
                    results=save_data['results'],
                    is_keywords=save_data['analyze_keywords']
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
                    messages.success(request,
                                     f'✅ Успешно добавлено {added_count} элементов в игру!'
                                     )

                    # Редирект с параметрами
                    redirect_url = reverse('analyze_game', args=[game.id]) + \
                                   f'?tab={active_tab}&mode={analyze_mode}&saved=1'
                    return redirect(redirect_url)
                else:
                    messages.error(request,
                                   f'❌ Не удалось сохранить результаты: {update_result.get("error", "Неизвестная ошибка")}')

            except Exception as e:
                messages.error(request, f'❌ Ошибка при сохранении: {str(e)}')

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


def highlight_matches_in_text(text: str, analysis_result: Dict, mode: str) -> str:
    """
    Подсвечивает найденные элементы в тексте с разными цветами для разных типов
    """
    if not text or not analysis_result.get('pattern_info'):
        return text

    from django.utils.html import escape

    # Экранируем HTML
    escaped_text = escape(text)

    # Собираем все совпадения с позициями
    matches = []

    if mode == 'keywords':
        keyword_matches = analysis_result.get('pattern_info', {}).get('keywords', [])
        for match in keyword_matches:
            if match.get('status') == 'found':
                matches.append({
                    'start': match.get('position', 0),
                    'end': match.get('position', 0) + len(match.get('matched_text', '')),
                    'type': 'keyword',
                    'name': match.get('name'),
                    'text': match.get('matched_text')
                })
    else:
        # Для критериев собираем из всех категорий
        categories = ['genres', 'themes', 'perspectives', 'game_modes']
        for category in categories:
            category_matches = analysis_result.get('pattern_info', {}).get(category, [])
            for match in category_matches:
                if match.get('status') == 'found':
                    matches.append({
                        'start': match.get('position', 0),
                        'end': match.get('position', 0) + len(match.get('matched_text', '')),
                        'type': category[:-1],  # Убираем 's' в конце
                        'name': match.get('name'),
                        'text': match.get('matched_text')
                    })

    # Сортируем по позиции (от конца к началу, чтобы не сбивать индексы)
    matches.sort(key=lambda x: x['start'], reverse=True)

    # Применяем подсветку
    highlighted_text = escaped_text
    for match in matches:
        before = highlighted_text[:match['start']]
        matched = highlighted_text[match['start']:match['end']]
        after = highlighted_text[match['end']:]

        # Создаем подсвеченный элемент с тултипом
        highlighted = (
            f'<mark class="highlight-{match["type"]}" '
            f'data-element-name="{escape(match["name"])}" '
            f'data-bs-toggle="tooltip" data-bs-title="{escape(match["name"])}">'
            f'{matched}'
            f'</mark>'
        )

        highlighted_text = before + highlighted + after

    return highlighted_text


def extract_found_items(analysis_result: Dict, mode: str, game=None) -> Dict:
    """
    Извлекает найденные элементы для отображения в боковой панели
    с указанием статуса (новый или уже есть)
    """
    if mode == 'keywords':
        keywords = analysis_result.get('results', {}).get('keywords', {}).get('items', [])
        result = []
        new_count = 0
        total_count = len(keywords)

        for k in keywords:
            is_new = True
            if game:
                is_new = not game.keywords.filter(id=k['id']).exists()
            if is_new:
                new_count += 1
            result.append({
                'name': k['name'],
                'id': k['id'],
                'is_new': is_new
            })

        return {
            'keywords': result,
            'new_count': new_count,
            'total_count': total_count
        }
    else:
        found = {}
        categories = ['genres', 'themes', 'perspectives', 'game_modes']
        new_counts = {}
        total_counts = {}

        for category in categories:
            items = analysis_result.get('results', {}).get(category, {}).get('items', [])
            if items:
                found_items = []
                new_count = 0
                total_count = len(items)

                for item in items:
                    is_new = True
                    if game:
                        if category == 'genres':
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

                found[category] = found_items
                new_counts[category] = new_count
                total_counts[category] = total_count
            else:
                new_counts[category] = 0
                total_counts[category] = 0

        return {
            'items': found,
            'new_counts': new_counts,
            'total_counts': total_counts
        }


@login_required
@user_passes_test(is_staff_or_superuser)
def clear_analysis_results(request: HttpRequest, game_id: int):
    """Очищает несохраненные результаты анализа"""
    game = get_object_or_404(Game, pk=game_id)

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

    # Получаем текущие параметры для редиректа
    active_tab = request.GET.get('tab', 'summary')
    analyze_mode = request.GET.get('mode', 'criteria')

    # Проверяем, это AJAX запрос или обычный
    if request.headers.get('x-requested-with') == 'XMLHttpRequest':
        # Для AJAX возвращаем JSON ответ
        return JsonResponse({
            'success': True,
            'message': f'Очищено {removed_count} результатов',
            'redirect_url': reverse('analyze_game', args=[game.id]) + f'?tab={active_tab}&mode={analyze_mode}&cleared=1'
        })

    # Для обычных запросов - редирект
    redirect_url = reverse('analyze_game', args=[game.id]) + f'?tab={active_tab}&mode={analyze_mode}&cleared=1'
    return redirect(redirect_url)
