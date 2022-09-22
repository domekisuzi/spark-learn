option = {
    backgroundColor: '#2c343c',
    title: {
        text: 'Customized Pie',
        left: 'center',
        top: 20,
        textStyle: {
            color: '#ccc'
        }
    },
    tooltip: {
        trigger: 'item'
    },
    visualMap: {
        show: false,
        min: 80,
        max: 600,
        inRange: {
            colorLightness: [0, 1]
        }
    },
    series: [
        {
            name: 'Access From',
            type: 'pie',
            radius: '55%',
            center: ['50%', '50%'],
            data: [
                {value: 335, name: 'Direct'},
                {value: 310, name: 'Email'},
                {value: 274, name: 'Union Ads'},
                {value: 235, name: 'Video Ads'},
                {value: 400, name: 'Search Engine'}
            ].sort(function (a, b) {
                return a.value - b.value;
            }),
            roseType: 'radius',
            label: {
                color: 'rgba(255, 255, 255, 0.3)'
            },
            labelLine: {
                lineStyle: {
                    color: 'rgba(255, 255, 255, 0.3)'
                },
                smooth: 0.2,
                length: 10,
                length2: 20
            },
            itemStyle: {
                color: '#c23531',
                shadowBlur: 200,
                shadowColor: 'rgba(0, 0, 0, 0.5)'
            },
            animationType: 'scale',
            animationEasing: 'elasticOut',
            animationDelay: function (idx) {
                return Math.random() * 200;
            }
        }
    ]
};

option.series.data = [
    {value: 335, name: 'Direct'},
    {value: 245, name: 'Email'},
    {value: 2612374, name: 'Union Ads'},
    {value: 231235, name: 'Video Ads'},
    {value: 4050, name: 'Search Engine'}
]
str = '[{"name":"0~60","number":123},{"name":"80~90","number":23},{"name":"90~100","number":152},{"name":"60~80","number":123}]'



function setPieChartData(data) {
    option.series.data = eval("(" + data + ")");



    console.log(option.series)
}

 data =[
    {  name: 'Direct',value: 335, },
    {   name: 'Email', value: 310},
    { name: 'Union Ads' ,value: 274  },
    { value: 235, name: 'Video Ads' },
    { value: 400, name: 'Search Engine' }
].sort(function (a, b) {
    return a.value - b.value;
})
console.log(data)

// setPieChartData('[{"name":"0~60","number":123},{"name":"80~90","number":23},{"name":"90~100","number":152},{"name":"60~80","number":123}]')