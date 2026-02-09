// games/static/games/js/game_list/game-pagination-loading.js
const GamePaginationLoading = {
    showSimpleLoadingIndicator(pageNumber) {
        const container = document.querySelector('.games-container');
        if (container) {
            // Только добавляем класс для легкой анимации, без затемнения
            container.classList.add('loading');
            console.log(`Added loading indicator for page ${pageNumber}`);
        }
    },

    removeSimpleLoadingIndicator() {
        const container = document.querySelector('.games-container');
        if (container) {
            container.classList.remove('loading');
            console.log('Removed loading indicator');
        }
    },

    showLoadingIndicator(pageNumber) {
        // УБРАЛИ создание темного экрана загрузки
        console.log(`Skipping dark loading screen for page ${pageNumber}`);

        // Вместо этого просто добавляем легкий класс loading
        const container = document.querySelector('.games-container');
        if (container) {
            container.classList.add('loading');
        }
    },

    removeLoadingIndicator(pageNumber) {
        // Убираем класс loading
        const container = document.querySelector('.games-container');
        if (container) {
            container.classList.remove('loading');
        }

        // Также удаляем любые оставшиеся индикаторы (на всякий случай)
        const existingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (existingIndicator) {
            console.log(`Removing loading indicator for page ${pageNumber}`);
            existingIndicator.remove();
            return true;
        }

        const allIndicators = document.querySelectorAll('.page-loading-indicator');
        if (allIndicators.length > 0) {
            console.log(`Removing ${allIndicators.length} orphaned loading indicators`);
            allIndicators.forEach(indicator => {
                indicator.remove();
            });
            return true;
        }

        console.log(`No loading indicator found for page ${pageNumber}`);
        return false;
    },

    removeTemplateLoadingIndicators() {
        const loadingMsg = document.querySelector('.games-loading-indicator');
        if (loadingMsg) {
            loadingMsg.remove();
            console.log('Removed template loading indicator');
        }
    }
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = GamePaginationLoading;
} else {
    window.GamePaginationLoading = GamePaginationLoading;
}

export default GamePaginationLoading;