$(document).ready(function() {
    $.ajax({
        url: "/profile",
        type: "GET",
        success: function(number) {
            const labels = ['1B', '1KB', '1MB', '1GB'];
            const data = {
                labels: labels,
                datasets: [{
                    label: 'Storage Profile',
                    data: number,
                    borderColor: 'rgb(255, 165, 0)',
                }]
            }
            const config = {
                type: 'line',
                data: data,
                options: {}
            };
            const chart = new Chart($("#profile-chart"), config);
        }
    });
});
