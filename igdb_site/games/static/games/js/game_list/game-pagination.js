// games/static/games/js/game_list/game-pagination.js
import GamePaginationCore from './game-pagination-core.js';
import GamePaginationUI from './game-pagination-ui.js';

const GamePagination = {
    ...GamePaginationCore,

    hidePagination() {
        if (GamePaginationUI && typeof GamePaginationUI.hidePagination === 'function') {
            GamePaginationUI.hidePagination();
        }
    },

    showPagination() {
        if (GamePaginationUI && typeof GamePaginationUI.showPagination === 'function') {
            GamePaginationUI.showPagination();
        }
    },

    updatePageInfoElements() {
        if (GamePaginationUI && typeof GamePaginationUI.updatePageInfoElements === 'function') {
            GamePaginationUI.updatePageInfoElements(
                this.config.currentPage,
                this.state.totalItems,
                this.state.totalPages,
                this.config.itemsPerPage
            );
        }
    },

    updateNavigationButtons() {
        if (GamePaginationUI && typeof GamePaginationUI.updateNavigationButtonPair === 'function') {
            GamePaginationUI.updateNavigationButtonPair('top');
            GamePaginationUI.updateNavigationButtonPair('bottom');
        }
    },

    updatePageButtonState(pageNumber, state) {
        if (GamePaginationUI && typeof GamePaginationUI.updatePageButtonState === 'function') {
            GamePaginationUI.updatePageButtonState(pageNumber, state);
        }
    },

    init() {
        console.log('=== GAME PAGINATION INIT (комбинированный) ===');

        GamePaginationCore.init.call(this);

        if (GamePaginationUI && typeof GamePaginationUI.init === 'function') {
            console.log('Initializing GamePagination UI...');
            GamePaginationUI.init();
        }
    }
};

window.GamePagination = GamePagination;
window.GamePaginationCore = GamePaginationCore;
window.GamePaginationUI = GamePaginationUI;

export default GamePagination;
export { GamePaginationCore, GamePaginationUI };