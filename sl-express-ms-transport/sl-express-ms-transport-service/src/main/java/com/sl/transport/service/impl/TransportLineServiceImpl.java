package com.sl.transport.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.itheima.em.sdk.EagleMapTemplate;
import com.itheima.em.sdk.enums.ProviderEnum;
import com.itheima.em.sdk.vo.Coordinate;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.domain.DispatchConfigurationDTO;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.domain.TransportLineNodeDTO;
import com.sl.transport.domain.TransportLineSearchDTO;
import com.sl.transport.entity.line.TransportLine;
import com.sl.transport.entity.node.AgencyEntity;
import com.sl.transport.entity.node.BaseEntity;
import com.sl.transport.entity.node.OLTEntity;
import com.sl.transport.entity.node.TLTEntity;
import com.sl.transport.enums.DispatchMethodEnum;
import com.sl.transport.enums.ExceptionEnum;
import com.sl.transport.enums.TransportLineEnum;
import com.sl.transport.repository.TransportLineRepository;
import com.sl.transport.service.CostConfigurationService;
import com.sl.transport.service.DispatchConfigurationService;
import com.sl.transport.service.OrganService;
import com.sl.transport.service.TransportLineService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @author analytics
 * @date 2024/10/22 23:01
 * @description
 */
@Service
public class TransportLineServiceImpl implements TransportLineService {
    @Resource
    private TransportLineRepository transportLineRepository;
    @Resource
    private OrganService organService;
    @Resource
    private EagleMapTemplate eagleMapTemplate;

    @Resource
    private DispatchConfigurationService dispatchConfigurationService;
    @Resource
    private CostConfigurationService costConfigurationService;


    @Override
    public Boolean createLine(TransportLine transportLine) {
        // 创建运输线路
        TransportLineEnum transportLineEnum = TransportLineEnum.codeOf(transportLine.getType());
        if (ObjectUtil.isNull(transportLineEnum)) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_TYPE_ERROR);
        }
        BaseEntity firstNode = null;
        BaseEntity secondNode = null;
        switch (transportLineEnum) {
            // 干线
            case TRUNK_LINE:
                firstNode = OLTEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = OLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            // 支线
            case BRANCH_LINE:
                firstNode = TLTEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = OLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            // 接驳路线
            case CONNECT_LINE:
                firstNode = AgencyEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = TLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            default:
                throw new SLException(ExceptionEnum.TRANSPORT_LINE_TYPE_ERROR);
        }
        if (ObjectUtil.hasNull(firstNode, secondNode)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        Long count = transportLineRepository.queryCount(firstNode, secondNode);
        if (count > 0) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_ALREADY_EXISTS);
        }
        // 补全路线属性
        transportLine.setId(null);
        transportLine.setCreated(System.currentTimeMillis());
        transportLine.setUpdated(System.currentTimeMillis());
        // 补全其他信息，地图信息
        infoFromMap(firstNode, secondNode, transportLine, transportLineEnum);
        count = transportLineRepository.create(firstNode, secondNode, transportLine);
        return count > 0;
    }

    private void infoFromMap(BaseEntity firstNode, BaseEntity secondNode, TransportLine transportLine, TransportLineEnum transportLineEnum) {
        OrganDTO start = organService.findByBid(firstNode.getBid());
        if (ObjectUtil.hasNull(start.getLongitude(), start.getLatitude())) {
            throw new SLException("请完善机构信息");
        }
        OrganDTO end = organService.findByBid(secondNode.getBid());
        if (ObjectUtil.hasNull(end.getLongitude(), end.getLatitude())) {
            throw new SLException("请完善机构信息");
        }
        Map<String, Object> param = MapUtil.<String, Object>builder().put("show_fields", "cost").build();
        Coordinate startCoordinate = new Coordinate(start.getLongitude(), start.getLatitude());
        Coordinate endCoordinate = new Coordinate(end.getLongitude(), end.getLatitude());
        String driving = eagleMapTemplate.opsForDirection().driving(ProviderEnum.AMAP, startCoordinate, endCoordinate, param);
        if (StrUtil.isBlank(driving)) {
            return;
        }
        JSONObject jsonObject = JSONUtil.parseObj(driving);
        // 计算时间
        Long time = Convert.toLong(jsonObject.getByPath("route.paths[0].cost.duration"), -1L);
        transportLine.setTime(time);
        // 计算距离
        Double distance = Convert.toDouble(jsonObject.getByPath("route.paths[0].distance"), -1D);
        transportLine.setDistance(distance);
        // 路线成本，计算费用
        double cost = NumberUtil.mul(NumberUtil.div(distance.doubleValue(), 1000D), transportLineEnum.getCost().doubleValue());
        transportLine.setCost(NumberUtil.round(cost, 2).doubleValue());
    }

    @Override
    public Boolean updateLine(TransportLine transportLine) {
        // 先查后改
        TransportLine transportLineData = this.queryById(transportLine.getId());
        if (null == transportLineData) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
        }
        //拷贝数据，忽略null值以及不能修改的字段
        BeanUtil.copyProperties(transportLine, transportLineData, CopyOptions.create().setIgnoreNullValue(true)
                .setIgnoreProperties("type", "startOrganId", "startOrganName", "endOrganId", "endOrganName"));
        transportLineData.setUpdated(System.currentTimeMillis());
        Long count = this.transportLineRepository.update(transportLineData);
        return count > 0;
    }

    @Override
    public Boolean deleteLine(Long id) {
        Long count = this.transportLineRepository.remove(id);
        return count > 0;
    }

    @Override
    public PageResponse<TransportLine> queryPageList(TransportLineSearchDTO transportLineSearchDTO) {
        return this.transportLineRepository.queryPageList(transportLineSearchDTO);
    }

    @Override
    public TransportLineNodeDTO queryShortestPath(Long startId, Long endId) {
        AgencyEntity start = AgencyEntity.builder().bid(startId).build();
        AgencyEntity end = AgencyEntity.builder().bid(endId).build();
        if (ObjectUtil.hasEmpty(start, end)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        return this.transportLineRepository.findShortestPath(start, end);
    }

    @Override
    public TransportLineNodeDTO findLowestPath(Long startId, Long endId) {
        AgencyEntity start = AgencyEntity.builder().bid(startId).build();
        AgencyEntity end = AgencyEntity.builder().bid(endId).build();

        if (ObjectUtil.hasEmpty(start, end)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        List<TransportLineNodeDTO> pathList = this.transportLineRepository.findPathList(start, end, 10, 1);
        if (CollUtil.isNotEmpty(pathList)) {
            return pathList.get(0);
        }
        return null;
    }

    @Override
    public TransportLineNodeDTO queryPathByDispatchMethod(Long startId, Long endId) {
        // 调度方式配置
        DispatchConfigurationDTO configuration = this.dispatchConfigurationService.findConfiguration();
        int method = configuration.getDispatchMethod();
        // 调度方式，1转运次数最少，2成本最低
        if (ObjectUtil.equal(DispatchMethodEnum.SHORTEST_PATH.getCode(), method)) {
            return this.queryShortestPath(startId, endId);
        } else {
            return this.findLowestPath(startId, endId);
        }
    }

    @Override
    public List<TransportLine> queryByIds(Long... ids) {
        return this.transportLineRepository.queryByIds(ids);
    }

    @Override
    public TransportLine queryById(Long id) {
        return this.transportLineRepository.queryById(id);
    }
}
