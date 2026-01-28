// games/static/games/js/analyze/utils.js
/**
 * Утилиты для Game Analyzer
 */

/* ============================================
   TAB PERSISTENCE UTILITIES
   ============================================ */

export function saveCurrentTab(analyzer) {
    // Сохраняем текущую вкладку в sessionStorage
    if (analyzer.currentTab) {
        sessionStorage.setItem(`analyze_active_tab_${analyzer.options.gameId}`, analyzer.currentTab);

        // Также сохраняем в localStorage для долгосрочного хранения
        localStorage.setItem(`analyze_active_tab_${analyzer.options.gameId}`, analyzer.currentTab);

        console.log(`Tab saved: ${analyzer.currentTab} for game ${analyzer.options.gameId}`);
    }
}

export function restoreCurrentTab(analyzer) {
    try {
        // Проверяем, есть ли вкладка в URL параметрах
        const urlParams = new URLSearchParams(window.location.search);
        const tabFromUrl = urlParams.get('tab');

        // Если есть вкладка в URL, используем её (самый высокий приоритет)
        if (tabFromUrl) {
            const tabExists = document.querySelector(`#analyzeTabs a[href="#${tabFromUrl}"]`);
            if (tabExists) {
                setTimeout(() => {
                    analyzer.switchTabByName(tabFromUrl);
                }, 100);
                return tabFromUrl;
            }
        }

        // Если нет в URL, пробуем получить из sessionStorage
        let savedTab = sessionStorage.getItem(`analyze_active_tab_${analyzer.options.gameId}`);

        // Если нет, пробуем из localStorage
        if (!savedTab) {
            savedTab = localStorage.getItem(`analyze_active_tab_${analyzer.options.gameId}`);
        }

        // Если есть сохраненная вкладка и она существует на странице
        if (savedTab) {
            const tabExists = document.querySelector(`#analyzeTabs a[href="#${savedTab}"]`);
            if (tabExists) {
                // Ждем немного, чтобы DOM полностью загрузился
                setTimeout(() => {
                    analyzer.switchTabByName(savedTab);
                }, 150);
                return savedTab;
            }
        }

        // Если ничего не нашли, но есть активная вкладка по умолчанию
        if (analyzer.currentTab) {
            saveCurrentTab(analyzer);
        }
    } catch (error) {
        console.error('Error restoring tab:', error);
    }

    return null;
}

export function clearTabStorage(analyzer) {
    // Очищаем сохраненные данные о вкладках
    sessionStorage.removeItem(`analyze_active_tab_${analyzer.options.gameId}`);
    localStorage.removeItem(`analyze_active_tab_${analyzer.options.gameId}`);
}

/* ============================================
   SCROLL POSITION UTILITIES
   ============================================ */

export function saveScrollPosition(analyzer) {
    // Сохраняем позицию прокрутки окна
    sessionStorage.setItem(`analyze_scroll_y_${analyzer.options.gameId}`, window.scrollY.toString());
}

export function restoreScrollPosition(analyzer) {
    try {
        const savedScrollY = sessionStorage.getItem(`analyze_scroll_y_${analyzer.options.gameId}`);
        if (savedScrollY) {
            const scrollY = parseInt(savedScrollY);
            if (scrollY > 0) {
                // Прокручиваем после небольшой задержки
                setTimeout(() => {
                    window.scrollTo(0, scrollY);
                    // Очищаем сохраненную позицию
                    sessionStorage.removeItem(`analyze_scroll_y_${analyzer.options.gameId}`);
                }, 100);
            }
        }
    } catch (error) {
        console.error('Error restoring scroll position:', error);
        sessionStorage.removeItem(`analyze_scroll_y_${analyzer.options.gameId}`);
    }
}

export function saveTabScrollPosition(analyzer, tabName) {
    if (!tabName) return;

    const tabPane = document.getElementById(tabName);
    if (!tabPane) return;

    const textDisplayArea = tabPane.querySelector('.text-display-area');
    if (textDisplayArea) {
        const scrollTop = textDisplayArea.scrollTop;

        // Сохраняем позицию прокрутки для конкретной вкладки
        sessionStorage.setItem(
            `analyze_tab_scroll_${analyzer.options.gameId}_${tabName}`,
            scrollTop.toString()
        );

        console.log(`Saved scroll position for tab ${tabName}: ${scrollTop}`);
    }
}

export function restoreTabScrollPosition(analyzer, tabName) {
    if (!tabName) return;

    const tabPane = document.getElementById(tabName);
    if (!tabPane) return;

    const textDisplayArea = tabPane.querySelector('.text-display-area');
    if (!textDisplayArea) {
        console.warn(`Text display area not found for tab ${tabName}`);
        return;
    }

    try {
        const savedScrollTop = sessionStorage.getItem(
            `analyze_tab_scroll_${analyzer.options.gameId}_${tabName}`
        );

        if (savedScrollTop) {
            const scrollTop = parseInt(savedScrollTop);
            if (scrollTop > 0) {
                // Устанавливаем позицию прокрутки
                textDisplayArea.scrollTop = scrollTop;
                console.log(`Restored scroll position for tab ${tabName}: ${scrollTop}`);

                // Очищаем сохраненную позицию после успешного восстановления
                setTimeout(() => {
                    sessionStorage.removeItem(`analyze_tab_scroll_${analyzer.options.gameId}_${tabName}`);
                }, 1000);
            }
        }
    } catch (error) {
        console.error(`Error restoring scroll position for tab ${tabName}:`, error);
        sessionStorage.removeItem(`analyze_tab_scroll_${analyzer.options.gameId}_${tabName}`);
    }
}

export function clearTabScrollPositions(analyzer) {
    // Очищаем все сохраненные позиции прокрутки для вкладок этой игры
    const prefix = `analyze_tab_scroll_${analyzer.options.gameId}_`;
    const keysToRemove = [];

    for (let i = 0; i < sessionStorage.length; i++) {
        const key = sessionStorage.key(i);
        if (key && key.startsWith(prefix)) {
            keysToRemove.push(key);
        }
    }

    keysToRemove.forEach(key => {
        sessionStorage.removeItem(key);
    });

    console.log(`Cleared ${keysToRemove.length} tab scroll positions for game ${analyzer.options.gameId}`);
}

/* ============================================
   DOM UTILITIES
   ============================================ */

export function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}