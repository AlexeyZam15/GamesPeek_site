# games/urls.py
from django.urls import path
from . import views

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', views.home, name='home'),
    path('search/', views.game_search, name='game_search'),
    path('games/', views.game_list, name='game_list'),
    path('games/<int:pk>/', views.game_detail, name='game_detail'),
    # URL для анализа - теперь импортируется из views_parts
    path('games/<int:game_id>/analyze/', views.analyze_single_game, name='analyze_game'),
    path('games/<int:game_id>/analyze/clear/', views.clear_analysis_results, name='clear_analysis_results'),
    path('games/compare/<int:pk2>/', views.game_comparison, name='game_comparison'),
    path('ajax/load-games-page/', views.ajax_load_games_page, name='ajax_load_games_page'),
    path('admin-auto-login/', views.auto_login_admin, name='auto_login_admin'),
    path('games/<int:game_id>/analyze/delete-keyword/', views.delete_keyword, name='delete_keyword'),
    path('games/<int:game_id>/analyze/current-keywords/', views.get_current_keywords, name='get_current_keywords'),
    path('games/<int:game_id>/analyze/found-items/', views.get_found_keywords, name='get_found_keywords'),
    # НОВЫЙ URL ДЛЯ НОРМАЛИЗАЦИИ
    path('games/normalize-keyword/', views.normalize_keyword, name='normalize_keyword'),

    # НОВЫЕ AJAX URL ДЛЯ ДОБАВЛЕНИЯ/УДАЛЕНИЯ КЛЮЧЕВЫХ СЛОВ БЕЗ ПЕРЕЗАГРУЗКИ
    path('games/<int:game_id>/analyze/add-keyword-ajax/', views.add_keyword_ajax, name='add_keyword_ajax'),
    path('games/<int:game_id>/analyze/delete-keyword-ajax/', views.delete_keyword_ajax, name='delete_keyword_ajax'),
    path('games/<int:game_id>/analyze/ajax/', views.analyze_game_ajax, name='analyze_game_ajax'),

    # НОВЫЙ URL ДЛЯ УДАЛЕНИЯ ТЕМЫ ИЗ ИГРЫ
    path('games/<int:game_id>/remove-theme/<int:theme_id>/', views.remove_theme_from_game,
         name='remove_theme_from_game'),
]

# ОБЯЗАТЕЛЬНО: добавляем статические и медиа файлы ТОЛЬКО в режиме DEBUG
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)