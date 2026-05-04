"""

Django signals for automatic maintenance of materialized vectors and cached counts.

Ensures Game.genre_ids, Game.keyword_ids etc. are always in sync with actual M2M relations.

Also invalidates GameCardCache when game data changes.

Also invalidates FilterSectionCache when filter entities change.

"""

from django.db.models.signals import m2m_changed, post_save, post_delete
from django.dispatch import receiver
from django.db import transaction
import logging
from .models import Game, GameCardCache, Genre, Keyword, Theme, PlayerPerspective, GameMode, Platform, GameEngine
from .models_parts.filter_cache import FilterSectionCache

logger = logging.getLogger(__name__)


@receiver(m2m_changed, sender=Game.genres.through)
@receiver(m2m_changed, sender=Game.themes.through)
@receiver(m2m_changed, sender=Game.player_perspectives.through)
@receiver(m2m_changed, sender=Game.developers.through)
@receiver(m2m_changed, sender=Game.game_modes.through)
@receiver(m2m_changed, sender=Game.platforms.through)
@receiver(m2m_changed, sender=Game.engines.through)
def update_game_vectors_on_m2m_change(sender, instance, action, reverse, model, pk_set, using, **kwargs):
    """
    Автоматически обновляет материализованные векторы (genre_ids, keyword_ids, ...)
    при любых изменениях ManyToMany связей игры.
    Также инвалидирует кэш карточки игры и кэш фильтров.

    Срабатывает только для действий, которые изменяют связи:
    - post_add: после добавления связей
    - post_remove: после удаления связей
    - post_clear: после очистки всех связей

    Использует transaction.on_commit для обновления ТОЛЬКО после успешного коммита транзакции,
    чтобы избежать race conditions и обновления несохраненных данных.
    """
    if action not in ['post_add', 'post_remove', 'post_clear']:
        return

    if reverse:
        if not pk_set:
            return
        games = Game.objects.filter(id__in=pk_set)
        for game in games:
            transaction.on_commit(
                lambda g=game: g.update_materialized_vectors(force=True)
            )
            transaction.on_commit(
                lambda g=game: invalidate_game_card_cache(g.id)
            )
            logger.debug(f"Scheduled vector update and card invalidation for game {game.id} via reverse M2M change")
    else:
        if not isinstance(instance, Game):
            return
        transaction.on_commit(
            lambda inst=instance: inst.update_materialized_vectors(force=True)
        )
        transaction.on_commit(
            lambda inst=instance: invalidate_game_card_cache(inst.id)
        )
        logger.debug(f"Scheduled vector update and card invalidation for game {instance.id} via direct M2M change")

    transaction.on_commit(invalidate_filter_caches)


@receiver(m2m_changed, sender=Game.genres.through)
@receiver(m2m_changed, sender=Game.platforms.through)
@receiver(m2m_changed, sender=Game.developers.through)
@receiver(m2m_changed, sender=Game.engines.through)
@receiver(m2m_changed, sender=Game.themes.through)
@receiver(m2m_changed, sender=Game.player_perspectives.through)
@receiver(m2m_changed, sender=Game.game_modes.through)
def update_cached_counts_on_m2m_change(sender, instance, action, reverse, model, pk_set, using, **kwargs):
    """
    Автоматически обновляет кэшированные счетчики (_cached_genre_count, _cached_keyword_count, ...)
    при изменениях ManyToMany связей.
    Также инвалидирует кэш карточки игры и кэш фильтров.

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
            transaction.on_commit(
                lambda g=game: invalidate_game_card_cache(g.id)
            )
    else:
        if not isinstance(instance, Game):
            return
        transaction.on_commit(
            lambda inst=instance: inst.update_cached_counts(force=False, async_update=True)
        )
        transaction.on_commit(
            lambda inst=instance: invalidate_game_card_cache(inst.id)
        )

    transaction.on_commit(invalidate_filter_caches)


@receiver(post_save, sender=Game)
def invalidate_card_on_game_save(sender, instance, **kwargs):
    """
    Инвалидирует кэш карточки игры при сохранении основных полей игры
    (имя, рейтинг, обложка, тип игры и т.д.)
    Также инвалидирует кэш фильтров при изменении игр.
    """
    transaction.on_commit(
        lambda: invalidate_game_card_cache(instance.id)
    )
    transaction.on_commit(invalidate_filter_caches)
    logger.debug(f"Scheduled card invalidation for game {instance.id} on save")


@receiver([post_save, post_delete], sender=Genre)
@receiver([post_save, post_delete], sender=Keyword)
@receiver([post_save, post_delete], sender=Theme)
@receiver([post_save, post_delete], sender=PlayerPerspective)
@receiver([post_save, post_delete], sender=GameMode)
@receiver([post_save, post_delete], sender=Platform)
@receiver([post_save, post_delete], sender=GameEngine)
def invalidate_filter_caches_on_entity_change(sender, **kwargs):
    """
    Инвалидирует кэш фильтров при изменении любой сущности,
    которая используется в фильтрах (жанры, ключевые слова, темы, платформы и т.д.)
    """
    logger.info(f"🔄 Entity {sender.__name__} changed, invalidating filter caches...")
    transaction.on_commit(invalidate_filter_caches)


def invalidate_game_card_cache(game_id: int) -> None:
    """
    Инвалидирует кэш карточки игры, помечая её как неактивную.
    """
    try:
        card = GameCardCache.objects.filter(game_id=game_id, is_active=True).first()
        if card:
            card.is_active = False
            card.save(update_fields=['is_active'])
            logger.info(f"Invalidated card cache for game {game_id}")
    except Exception as e:
        logger.error(f"Error invalidating card cache for game {game_id}: {e}")


def invalidate_filter_caches() -> None:
    """
    Инвалидирует все кэши фильтров, помечая их как неактивные.
    Вызывается при изменении любых данных, влияющих на фильтры.
    """
    try:
        count = FilterSectionCache.invalidate_all_filters()
        logger.info(f"Invalidated {count} filter section caches")
    except Exception as e:
        logger.error(f"Error invalidating filter caches: {e}")
