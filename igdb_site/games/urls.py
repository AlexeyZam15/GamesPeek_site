# games/urls.py
from django.urls import path
from . import views
from . import analyze_views  # Импортируем новые views

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', views.home, name='home'),
    path('search/', views.game_search, name='game_search'),
    path('games/', views.game_list, name='game_list'),
    path('games/<int:pk>/', views.game_detail, name='game_detail'),
    # URL для анализа
    path('games/<int:game_id>/analyze/', analyze_views.analyze_single_game, name='analyze_game'),
    path('games/<int:game_id>/analyze/clear/', analyze_views.clear_analysis_results, name='clear_analysis_results'),
    path('games/compare/<int:pk2>/', views.game_comparison, name='game_comparison'),
    path('platforms/', views.platform_list, name='platform_list'),
    path('platforms/<int:platform_id>/games/', views.platform_games, name='platform_games'),
    path('ajax/load-games-page/', views.ajax_load_games_page, name='ajax_load_games_page'),
    path('test-pagination/', views.test_pagination, name='test_pagination'),
]

# ОБЯЗАТЕЛЬНО: добавляем статические и медиа файлы ТОЛЬКО в режиме DEBUG
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)