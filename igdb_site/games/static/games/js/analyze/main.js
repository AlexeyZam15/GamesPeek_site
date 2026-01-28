// games/static/games/js/analyze/main.js
/**
 * Главный файл инициализации Game Analyzer
 */

import GameAnalyzerUI from './GameAnalyzerUI.js';

/* ============================================
   GLOBAL INITIALIZATION
   ============================================ */

document.addEventListener('DOMContentLoaded', function() {
    try {
        console.log('=== GAME ANALYZER INITIALIZATION ===');

        // Создаем экземпляр анализатора
        const analyzer = new GameAnalyzerUI();
        window.gameAnalyzer = analyzer;

        // Дополнительные обработчики
        window.addEventListener('resize', () => {
            setTimeout(() => {
                analyzer.forceTextAlignmentFix();
            }, 100);
        });

        // Обработка перед закрытием страницы
        window.addEventListener('beforeunload', () => {
            // Сохраняем текущую вкладку и позицию прокрутки перед закрытием
            analyzer.saveCurrentTab();
            analyzer.saveScrollPosition();
            if (analyzer.currentTab) {
                analyzer.saveTabScrollPosition(analyzer.currentTab);
            }
        });

        // Также сохраняем позицию при разгрузке страницы (refresh, navigation)
        window.addEventListener('pagehide', () => {
            analyzer.saveCurrentTab();
            analyzer.saveScrollPosition();
            if (analyzer.currentTab) {
                analyzer.saveTabScrollPosition(analyzer.currentTab);
            }
        });

        console.log('✅ Game Analyzer initialized successfully');

    } catch (error) {
        console.error('❌ Failed to initialize Game Analyzer UI:', error);

        // Fallback сообщение об ошибке
        const errorAlert = document.createElement('div');
        errorAlert.className = 'alert alert-danger alert-dismissible fade show position-fixed';
        errorAlert.style.cssText = `
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1060;
            min-width: 300px;
            max-width: 500px;
            text-align: center;
        `;
        errorAlert.innerHTML = `
            <i class="bi bi-x-circle me-2"></i>
            Failed to initialize analyzer. Please refresh the page.
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(errorAlert);
    }
});

// Экспортируем для возможного использования в других модулях
export { GameAnalyzerUI };