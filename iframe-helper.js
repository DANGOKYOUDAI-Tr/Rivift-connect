document.addEventListener('DOMContentLoaded', () => {
    const scriptSrc = document.currentScript.src;
    const trustedOrigin = new URL(scriptSrc).origin;

    document.body.addEventListener('click', (e) => {
        const link = e.target.closest('a');
        if (link && link.href) {
            e.preventDefault();
            e.stopPropagation();
            window.parent.postMessage({
                type: 'rivift-browser-nav',
                url: link.href 
            }, trustedOrigin); 
        }
    }, true); 
});