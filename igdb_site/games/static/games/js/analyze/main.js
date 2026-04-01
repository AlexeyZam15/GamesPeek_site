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

        let gameId = null;

        const gameIdInput = document.getElementById('game-id');
        if (gameIdInput && gameIdInput.value) {
            gameId = gameIdInput.value;
            console.log('Game ID from hidden input:', gameId);
        }

        if (!gameId) {
            const urlMatch = window.location.pathname.match(/\/games\/(\d+)\/analyze/);
            if (urlMatch && urlMatch[1]) {
                gameId = urlMatch[1];
                console.log('Game ID from URL:', gameId);
            }
        }

        if (!gameId) {
            const analyzeContainer = document.querySelector('.analyze-page');
            if (analyzeContainer && analyzeContainer.dataset.gameId) {
                gameId = analyzeContainer.dataset.gameId;
                console.log('Game ID from data attribute:', gameId);
            }
        }

        if (!gameId) {
            console.error('CRITICAL: Could not determine Game ID!');
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
                Could not determine Game ID. Please refresh the page.
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            `;
            document.body.appendChild(errorAlert);
            return;
        }

        const originalAddEventListener = EventTarget.prototype.addEventListener;
        EventTarget.prototype.addEventListener = function(type, listener, options) {
            const wrappedListener = function(e) {
                try {
                    return listener.apply(this, arguments);
                } catch (error) {
                    console.error(`Error in event listener for ${type}:`, error);
                    return null;
                }
            };
            return originalAddEventListener.call(this, type, wrappedListener, options);
        };

        const analyzer = new GameAnalyzerUI({
            gameId: gameId,
            apiUrl: '/'
        });
        window.gameAnalyzer = analyzer;

        window.addEventListener('resize', () => {
            setTimeout(() => {
                analyzer.forceTextAlignmentFix();
            }, 100);
        });

        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        tabLinks.forEach(link => {
            link.addEventListener('hidden.bs.tab', () => {
                setTimeout(() => {
                    analyzer.setupTooltips();
                }, 50);
            });
        });

        window.addEventListener('beforeunload', () => {
            analyzer.saveCurrentTab();
            analyzer.saveScrollPosition();
            if (analyzer.currentTab) {
                analyzer.saveTabScrollPosition(analyzer.currentTab);
            }
        });

        window.addEventListener('pagehide', () => {
            analyzer.saveCurrentTab();
            analyzer.saveScrollPosition();
            if (analyzer.currentTab) {
                analyzer.saveTabScrollPosition(analyzer.currentTab);
            }
        });

        console.log('✅ Game Analyzer initialized successfully with gameId:', gameId);

    } catch (error) {
        console.error('❌ Failed to initialize Game Analyzer UI:', error);

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

export { GameAnalyzerUI };