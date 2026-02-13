"""
Django signals for automatic maintenance of materialized vectors and cached counts.
Ensures Game.genre_ids, Game.keyword_ids etc. are always in sync with actual M2M relations.
"""

from django.db.models.signals import m2m_changed
from django.dispatch import receiver
from django.db import transaction
import logging

from .models import Game

logger = logging.getLogger(__name__)


@receiver(m2m_changed, sender=Game.genres.through)
@receiver(m2m_changed, sender=Game.keywords.through)
@receiver(m2m_changed, sender=Game.themes.through)
@receiver(m2m_changed, sender=Game.player_perspectives.through)
@receiver(m2m_changed, sender=Game.developers.through)
@receiver(m2m_changed, sender=Game.game_modes.through)
def update_game_vectors_on_m2m_change(sender, instance, action, reverse, model, pk_set, using, **kwargs):
    """
    Автоматически обновляет материализованные векторы (genre_ids, keyword_ids, ...)
    при любых изменениях ManyToMany связей игры.

    Срабатывает только для действий, которые изменяют связи:
    - post_add: после добавления связей
    - post_remove: после удаления связей
    - post_clear: после очистки всех связей

    Использует transaction.on_commit для обновления ТОЛЬКО после успешного коммита транзакции,
    чтобы избежать race conditions и обновления несохраненных данных.
    """
    # Обновляем векторы только при фактических изменениях данных
    if action not in ['post_add', 'post_remove', 'post_clear']:
        return

    # Если reverse=True, instance - это не Game (например Genre), получаем игры через pk_set
    if reverse:
        if not pk_set:
            return

        # Получаем все игры, которые были затронуты
        games = Game.objects.filter(id__in=pk_set)

        for game in games:
            # Обновляем после коммита транзакции
            transaction.on_commit(
                lambda g=game: g.update_materialized_vectors(force=True)
            )
            logger.debug(f"Scheduled vector update for game {game.id} via reverse M2M change")
    else:
        # Прямое изменение - instance это Game
        if not isinstance(instance, Game):
            return

        # Обновляем после коммита транзакции
        transaction.on_commit(
            lambda inst=instance: inst.update_materialized_vectors(force=True)
        )
        logger.debug(f"Scheduled vector update for game {instance.id} via direct M2M change")


@receiver(m2m_changed, sender=Game.genres.through)
@receiver(m2m_changed, sender=Game.keywords.through)
@receiver(m2m_changed, sender=Game.platforms.through)
@receiver(m2m_changed, sender=Game.developers.through)
def update_cached_counts_on_m2m_change(sender, instance, action, reverse, model, pk_set, using, **kwargs):
    """
    Автоматически обновляет кэшированные счетчики (_cached_genre_count, _cached_keyword_count, ...)
    при изменениях ManyToMany связей.

    Это дополняет существующий механизм в save() и обеспечивает актуальность счетчиков
    при изменениях через админку или массовые операции.
    """
    if action not in ['post_add', 'post_remove', 'post_clear']:
        return

    if reverse:
        if not pk_set:
            return

        games = Game.objects.filter(id__in=pk_set)

        for game in games:
            transaction.on_commit(
                lambda g=game: g.update_cached_counts(force=False, async_update=True)
            )
    else:
        if not isinstance(instance, Game):
            return

        transaction.on_commit(
            lambda inst=instance: inst.update_cached_counts(force=False, async_update=True)
        )