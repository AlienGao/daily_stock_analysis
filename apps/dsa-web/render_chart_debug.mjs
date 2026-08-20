import * as echarts from 'echarts/core';
import { BarChart } from 'echarts/charts';
import { GridComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
echarts.use([SVGRenderer, BarChart, GridComponent]);
const chart = echarts.init(null, null, { renderer: 'svg', ssr: true, width: 590, height: 200 });
chart.setOption({ xAxis: { type: 'category', data: ['a','b','c'] }, yAxis: {}, series: [{ type: 'bar', data: [1,2,3] }] });
console.log(chart.renderToSVGString().length);
