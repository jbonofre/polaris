import React, { useState } from 'react';
import { Layout, Input, Col, Row, Image, Menu, Space } from 'antd';
import { UserOutlined, BlockOutlined, SettingOutlined, DeleteOutlined, PlayCircleOutlined, LogoutOutlined, ApartmentOutlined, DashboardOutlined, AreaChartOutlined, NotificationOutlined, SafetyOutlined, TeamOutlined, BuildOutlined, CrownOutlined, ProfileOutlined, CheckSquareOutlined } from '@ant-design/icons';

function SideMenu() {

    const[ collapsed, setCollapsed ] = useState(false);

    const mainMenu = [
        { key: 'catalogs', label: 'Catalogs', icon: <BlockOutlined/>, children: [
            { key: 'default', label: 'Default', icon: <ApartmentOutlined/> },
            { key: 'my', label: 'My Catalog', icon: <ApartmentOutlined/> }
        ] },
        { key: 'governance', label: 'Governance', icon: <SafetyOutlined/>, children: [
            { key: 'principals', label: 'Principals', icon: <UserOutlined/> },
            { key: 'principal_roles', label: 'Principal Roles', icon: <TeamOutlined/> },
            { key: 'catalog_roles', label: 'Catalog Roles', icon: <BuildOutlined/> },
            { key: 'privileges', label: 'Privileges', icon: <CrownOutlined/> }
        ]},
        { key: 'policies', label: 'Policies & TMS', icon: <CheckSquareOutlined/> },
        { key: 'observe', label: 'Observability', icon: <DashboardOutlined/>, children: [
            { key: 'metrics', label: 'Metrics', icon: <AreaChartOutlined/> },
            { key: 'events', label: 'Events', icon: <NotificationOutlined/> }
        ]},
        { key: 'profiles', label: 'Profiles', icon: <ProfileOutlined/> },
        { key: 'settings', label: 'Settings', icon: <SettingOutlined/>, children: [
            { key: 'bootstrap', label: 'Bootstrap', icon: <PlayCircleOutlined/> },
            { key: 'purge', label: 'Purge', icon: <DeleteOutlined/> }
        ] }
    ];

    return(
        <Layout.Sider collapsible={true} collapsed={collapsed} onCollapse={newValue => setCollapsed(newValue)}>
            <Menu items={mainMenu} mode="inline"/>
        </Layout.Sider>
    );

}

// TODO add props here
function Header() {

    const { Search } = Input;

    const userMenu = [
        { key: 'admin', label: 'Admin', icon: <UserOutlined/>, children: [
            { key: 'preferences', label: 'Preferences', icon: <SettingOutlined/> },
            { key: 'logout', label: 'Logout', icon: <LogoutOutlined/> }
        ] }
    ];

    return(
        <Layout.Header style={{ height: "80px", background: "#fff", padding: "5px", margin: "10px" }}>
            <Row align="middle" justify="center" wrap="false">
                <Col span={3}><Space><Image src="./logo.png" preview={false} width={60}/> <span><b>Apache Polaris</b></span></Space></Col>
                <Col span={19}><Search /></Col>
                <Col span={2}><Menu items={userMenu} /></Col>
            </Row>
        </Layout.Header>
    );

}

// TODO move in a component
function Home() {

    return(
      "Hello"
    );

}

export default function Workspace() {

    return(
        <Layout style={{ height: "105vh" }}>
            <Header />
            <Layout hasSider={true}>
                <SideMenu />
                <Layout.Content style={{ margin: "15px" }}>
                    <Home />
                </Layout.Content>
            </Layout>
            <Layout.Footer>Apache®, Apache Polaris™ are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.</Layout.Footer>
        </Layout>
    );

}