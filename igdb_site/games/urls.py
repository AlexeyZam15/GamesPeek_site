from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('search/', views.game_search, name='game_search'),
    path('games/', views.game_list, name='game_list'),
    path('games/<int:pk>/', views.game_detail, name='game_detail'),
    path('games/compare/<int:pk2>/', views.game_comparison, name='game_comparison'),  # Один универсальный путь
]