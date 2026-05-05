import { createRoot } from 'react-dom/client'
import { App as AntdApp, ConfigProvider, theme } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import { useTheme } from 'next-themes'
import dayjs from 'dayjs'
import 'dayjs/locale/zh-cn'
import './index.css'
import App from './App.tsx'
import { ThemeProvider } from './components/theme/ThemeProvider'

dayjs.locale('zh-cn')

function AntdThemeProvider({ children }: { children: React.ReactNode }) {
  const { resolvedTheme } = useTheme()
  const isDark = resolvedTheme === 'dark'
  return (
    <ConfigProvider
      locale={zhCN}
      theme={{
        algorithm: isDark ? theme.darkAlgorithm : theme.defaultAlgorithm,
        token: { colorPrimary: '#00d4ff' },
      }}
    >
      <AntdApp>{children}</AntdApp>
    </ConfigProvider>
  )
}

createRoot(document.getElementById('root')!).render(
    <ThemeProvider>
      <AntdThemeProvider>
        <App />
      </AntdThemeProvider>
    </ThemeProvider>,
)
