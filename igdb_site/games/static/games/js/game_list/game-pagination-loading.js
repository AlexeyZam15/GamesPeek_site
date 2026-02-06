// games/static/games/js/game_list/game-pagination-loading.js
// Новый файл с методами для индикаторов загрузки

const GamePaginationLoading = {
    showSimpleLoadingIndicator(pageNumber) {
        const container = document.querySelector('.games-container');
        if (container) {
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
        const existingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (existingIndicator) {
            console.log(`Loading indicator for page ${pageNumber} already exists`);
            return;
        }

        const container = document.querySelector('.games-container');
        if (!container) return;

        console.log(`Creating loading indicator for page ${pageNumber}`);

        const loadingDiv = document.createElement('div');
        loadingDiv.className = 'page-loading-indicator';
        loadingDiv.id = `loading-page-${pageNumber}`;
        loadingDiv.innerHTML = `
            <div class="text-center py-5">
                <div class="spinner-border text-primary mb-3" style="width: 3rem; height: 3rem;" role="status">
                    <span class="visually-hidden">Loading page ${pageNumber}...</span>
                </div>
                <h5>Loading page ${pageNumber}...</h5>
                <p class="text-muted">Please wait while the games are loading</p>
            </div>
        `;
        loadingDiv.style.cssText = `
            background: rgba(255, 255, 255, 0.9);
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            z-index: 1000;
            display: flex;
            align-items: center;
            justify-content: center;
        `;

        let rowElement = container.querySelector('.row');
        if (!rowElement) {
            rowElement = document.createElement('div');
            rowElement.className = 'row';
            container.appendChild(rowElement);
        }

        loadingDiv.style.position = 'absolute';
        loadingDiv.style.zIndex = '1000';

        rowElement.style.position = 'relative';

        rowElement.appendChild(loadingDiv);
    },

    removeLoadingIndicator(pageNumber) {
        const loadingIndicator = document.getElementById(`loading-page-${pageNumber}`);
        if (loadingIndicator) {
            console.log(`Removing loading indicator for page ${pageNumber}`);
            loadingIndicator.remove();
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