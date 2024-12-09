package com.sl.gateway.filter;

import cn.hutool.core.collection.CollUtil;
import com.itheima.auth.factory.AuthTemplateFactory;
import com.itheima.auth.sdk.AuthTemplate;
import com.itheima.auth.sdk.common.Result;
import com.itheima.auth.sdk.dto.AuthUserInfoDTO;
import com.itheima.auth.sdk.service.TokenCheckService;
import com.sl.gateway.config.MyConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;

/**
 * 快递员token拦截处理
 */
@Component
@Slf4j
public class CourierTokenGatewayFilterFactory extends AbstractGatewayFilterFactory<Object> implements AuthFilter {

    @Resource
    private MyConfig myConfig;
    @Resource
    private TokenCheckService tokenCheckService;

    @Value("${role.courier}")
    private List<Long> courierRoleIds;

    @Override
    public GatewayFilter apply(Object config) {
        return new TokenGatewayFilter(this.myConfig, this);
    }

    @Override
    public AuthUserInfoDTO check(String token) {
        // 校验token
        return tokenCheckService.parserToken(token);
    }

    @Override
    public Boolean auth(String token, AuthUserInfoDTO authUserInfoDTO, String path) {
        AuthTemplate authTemplate = AuthTemplateFactory.get(token);
        Result<List<Long>> roleIdResult = authTemplate.opsForRole().findRoleByUserId(authUserInfoDTO.getUserId());
        List<Long> roleIds = roleIdResult.getData();
        Collection<Long> intersection = CollUtil.intersection(courierRoleIds, roleIds);
        return CollUtil.isNotEmpty(intersection);
    }
}
