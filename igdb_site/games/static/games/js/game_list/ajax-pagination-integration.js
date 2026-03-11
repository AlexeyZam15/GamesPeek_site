// games/static/games/js/game_list/ajax-pagination-integration.js
/**
 * Интеграция AJAX-пагинации с фильтрами
 * Добавляет в FilterManager методы для работы с пагинацией и очистку кэша
 */

(function() {
    // Сохраняем ссылку на оригинальный метод submitForm
    const originalSubmitForm = window.FilterManager?.submitForm;

    if (window.FilterManager) {
        console.log('AjaxPagination: Integrating with FilterManager...');

        // Переопределяем submitForm для сброса пагинации и очистки кэша при применении фильтров
        window.FilterManager.submitForm = function(e) {
            // Очищаем кэш страниц перед применением фильтров
            if (window.AjaxPagination && typeof window.AjaxPagination.clearCache === 'function') {
                console.log('AjaxPagination: Clearing page cache before applying filters');
                window.AjaxPagination.clearCache();
            }

            // Вызываем оригинальный метод
            if (originalSubmitForm) {
                const result = originalSubmitForm.apply(this, arguments);

                // Сбрасываем пагинацию на первую страницу
                if (window.AjaxPagination && window.AjaxPagination.state?.isInitialized) {
                    console.log('AjaxPagination: Filters applied, resetting to page 1');

                    setTimeout(() => {
                        if (window.FilterManager?.useAjax) {
                            window.AjaxPagination.reset();
                        }
                    }, 100);
                }

                return result;
            }
        };

        // Добавляем метод для принудительной очистки кэша
        window.FilterManager.clearPaginationCache = function() {
            if (window.AjaxPagination && typeof window.AjaxPagination.clearCache === 'function') {
                window.AjaxPagination.clearCache();
                console.log('AjaxPagination: Cache manually cleared via FilterManager');
                return true;
            }
            return false;
        };

        console.log('AjaxPagination: FilterManager integration complete');
    }

    // Также очищаем кэш при изменении сортировки
    document.addEventListener('click', function(e) {
        const sortLink = e.target.closest('[data-sort]');
        if (sortLink && window.AjaxPagination) {
            console.log('AjaxPagination: Sort changed, clearing cache');
            window.AjaxPagination.clearCache();
        }
    });

    // Экспорт для использования в других модулях
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = { integrateWithFilters: true };
    }
})();