package com.scb.pbwm.omf.order.client.oly;

import com.scb.pbwm.omf.core.exception.ApiErrorResponseBody;
import com.scb.pbwm.omf.core.exception.ApiException;
import com.scb.pbwm.omf.core.logging.LogUtil;
import com.scb.pbwm.omf.core.util.DateUtil;
import com.scb.pbwm.omf.core.util.ThreadLocalStore;
import com.scb.pbwm.omf.core.util.UserContext;
import com.scb.pbwm.omf.order.client.EncryptionRestClient;
import com.scb.pbwm.omf.order.client.oly.constant.OrderQueueStatusEnum;
import com.scb.pbwm.omf.order.client.oly.domain.response.OlyOrderStatusResponse;
import com.scb.pbwm.omf.order.client.oly.domain.response.common.Order;
import com.scb.pbwm.omf.order.client.oly.util.OlyUtil;
import com.scb.pbwm.omf.order.configuration.OlyKongProperties;
import com.scb.pbwm.omf.order.domain.*;
import com.scb.pbwm.omf.order.exception.OrderApiErrorCodes;
import com.scb.pbwm.omf.order.repository.OrderRepository;
import com.scb.pbwm.omf.order.service.oAuth.OAuthService;
import com.scb.pbwm.omf.order.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.scb.pbwm.omf.order.client.util.ClientConstant.*;

@Service
@Slf4j
public class OlyOrderStatusEnquiryClient {

    private String tokenScope;
    private LogUtil logUtil;
    private DateUtil dateUtil;
    private OlyKongProperties olyKongProperties;
    private OAuthService oAuthService;
    private OlyOrderStatusEnquiryFeignClient olyOrderStatusEnquiryFeignClient;
    private OrderRepository orderRepository;
    private EncryptionRestClient encryptionRestClient;

    private OlyUtil olyUtil;

    @Autowired
    public OlyOrderStatusEnquiryClient(@Value("${kong.oly.token-scope.order}") String tokenScope,
                                       LogUtil logUtil,
                                       DateUtil dateUtil,
                                       OlyKongProperties olyKongProperties,
                                       OAuthService oAuthService,
                                       OlyOrderStatusEnquiryFeignClient olyOrderStatusEnquiryFeignClient,
                                       OrderRepository orderRepository,
                                       EncryptionRestClient encryptionRestClient,
                                       OlyUtil olyUtil) {
        this.tokenScope = tokenScope;
        this.logUtil = logUtil;
        this.dateUtil = dateUtil;
        this.olyKongProperties = olyKongProperties;
        this.oAuthService = oAuthService;
        this.olyOrderStatusEnquiryFeignClient = olyOrderStatusEnquiryFeignClient;
        this.orderRepository = orderRepository;
        this.encryptionRestClient = encryptionRestClient;
        this.olyUtil = olyUtil;
    }

    public List<Orders> getOrders(String activeOrders, String portfolioId, String fromDate, String toDate, String transactionType, String assetCode, Integer pageNo, Integer pageSize) {
        List<Orders> ordersList = new ArrayList<>();
        UserContext context = ThreadLocalStore.getInstance().getUserContext();
        String relationshipId = context.getRelId();
        String correlationId = UUID.randomUUID().toString();
        Date startTime = new Date();
        try {
            log.info("OlyOrderStatusEnquiryClient:getOrders: {}", logUtil.printLog(Constants.INTERFACE_NAME_OLY, transactionType));

            OAuthTokenResponse token = oAuthService.getOauthToken(tokenScope);
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put("Accept", "application/json");
            headerMap.put("Authorization", "Bearer " + token.getAccessToken());
            headerMap.put("sourceSystem", olyKongProperties.getSourceSystem());
            headerMap.put("ctryCode", olyKongProperties.getCountryCode());
            headerMap.put("segmentCode", olyKongProperties.getSegmentCode());
            headerMap.put("productCode", olyKongProperties.getProductCode());
            headerMap.put("correlationId", correlationId);
            headerMap.put("reqDateTime", dateUtil.convertDateToXmlGregorianCalendar(new Date()).toString());
            headerMap.put("requestType", olyKongProperties.getRequestType());
            headerMap.put("userId", olyKongProperties.getUserId());

            //Leo: we required to combine the data from T24 API (In progress) and OMF Database (Failed or COB Pending)
            // Considering Active Orders most of time total record will not be a lot (less than 20)
            // In additional reduce the complexity for handle pagination for combine data, then we will get all data from API first then combine with database.
            Integer maxPageSize = olyKongProperties.getOrderStatusPageSize();

            Integer currentPage = 1;
            log.info("OLY_T24_PT Api getActiveOrders Start with correlationId : {} , id {} ,pageNo {} , pageSize {}",correlationId, relationshipId, pageNo,
                    pageSize);
            OlyOrderStatusResponse response = olyOrderStatusEnquiryFeignClient.getOrders(
                    olyKongProperties.getIdType(), relationshipId,
                    currentPage,
                    maxPageSize, headerMap);

            long totalTimeTaken = new Date().getTime() - startTime.getTime();
            log.info("OLY_T24_PT Api getActiveOrders End correlationId : {} , Time Taken (ms) : {} " , correlationId,totalTimeTaken);

            List<Order> orders = response.getOrderDetails();
            if(response.getPage()!=null
                    && response.getPage().getTotalRecords()!=null){
                Integer maxRecord = response.getPage().getTotalRecords();

                while (maxRecord>(maxPageSize*currentPage)){
                    currentPage = currentPage+1;
                    OlyOrderStatusResponse AdditionalResponse = olyOrderStatusEnquiryFeignClient.getOrders(
                            olyKongProperties.getIdType(), relationshipId,
                            currentPage,
                            maxPageSize, headerMap);
                    orders = Stream.concat(orders.stream(), AdditionalResponse.getOrderDetails().stream()).collect(Collectors.toList());
                }
            }


            for (Order respOrder : orders) {
                TransactionTypeEnum omfTransactTypeEnum = olyUtil.getOMFTransactTypeEnum(respOrder.getTxnType(), respOrder.getSettleMethod(), respOrder.getTxnChannel());
                if(TransactionTypeEnum.BCF.equals(omfTransactTypeEnum)
                        ||TransactionTypeEnum.BSF.equals(omfTransactTypeEnum)
                        ||TransactionTypeEnum.SSF.equals(omfTransactTypeEnum)
                        ||TransactionTypeEnum.SCF.equals(omfTransactTypeEnum))
                {
                    continue;
                }
                Orders orderStatus = new Orders();
                orderStatus.setActiveOrders(activeOrders);
                orderStatus.setInvestmentAccountNo(respOrder.getPortfolioAccNo());
                orderStatus.setInvestmentAccountProductType(respOrder.getPortfolioAccType());
                orderStatus.setOrderChanelCode(respOrder.getTxnChannel());
                orderStatus.setOmfOrderNo(respOrder.getChnlOrderNum());
//                orderStatus.setOrderNo(respOrder.getOrderNum());
                orderStatus.setOrderNo(respOrder.getChnlOrderNum());

                if (respOrder.getOrderDate() != null) {
                    orderStatus.setOrderDate(dateUtil.formatDate(respOrder.getOrderDate(), Constants.YYYYMMDD));
                }
                if (respOrder.getTxnDate() != null) {
                    orderStatus.setTransactionDate(dateUtil.formatDate(respOrder.getTxnDate(), Constants.YYYYMMDD));
                }


                orderStatus.setTransactionType(omfTransactTypeEnum);

                if (StringUtils.isBlank(respOrder.getDivInstr())) {
                    orderStatus.setDividendInstruction(DividendInstructionEnum.B);
                } else if (Arrays.stream(DividendInstructionEnum.values()).anyMatch(e -> e.name().equals(respOrder.getDivInstr()))) {
                    orderStatus.setDividendInstruction(DividendInstructionEnum.valueOf(respOrder.getDivInstr()));
                } else {
                    orderStatus.setDividendInstruction(DividendInstructionEnum.UNKNOWN);
                }

                if (Arrays.stream(OrderStatusEnum.values()).anyMatch(e -> e.getValue().equals(respOrder.getOrderStatus()))) {
                    orderStatus.setOrderStatus(OrderStatusEnum.fromValue(respOrder.getOrderStatus()).getStatusDesc());
                } else {
                    orderStatus.setOrderStatus(OrderStatusEnum.UNKNOWN.getStatusDesc());
                }

                orderStatus.setFundCode(respOrder.getAssetCode());
                orderStatus.setOrderCcy(respOrder.getOrderCcyCode());
                orderStatus.setOrderAmount(respOrder.getOrderAmt());
                orderStatus.setOrderUnit(respOrder.getOrderUnit());
                orderStatus.setCoolOffIndicator(respOrder.getCoolCancelPeriod());
                orderStatus.setSettlementMethod(respOrder.getSettleMethod());
                orderStatus.setOrderChanelCode(olyUtil.convertToOrderChannelCode(respOrder.getTxnChannel()));
                if (orderStatus.getTransactionType() == TransactionTypeEnum.SUB
                        || orderStatus.getTransactionType() == TransactionTypeEnum.BCF
                        || orderStatus.getTransactionType() == TransactionTypeEnum.BSF) {
                    orderStatus.setSettlementAccountNo(respOrder.getDebitSettleAcctNum());
                    orderStatus.setSettlementAccountCurrency(respOrder.getDebitSettleAcctCcyCode());
                } else if (orderStatus.getTransactionType() == TransactionTypeEnum.RED
                        || orderStatus.getTransactionType() == TransactionTypeEnum.SCF
                        || orderStatus.getTransactionType() == TransactionTypeEnum.SSF) {
                    orderStatus.setSettlementAccountNo(respOrder.getCreditSettleAcctNum());
                    orderStatus.setSettlementAccountCurrency(respOrder.getCreditSettleAcctCcyCode());
                } else {
                    orderStatus.setSettlementAccountNo("");
                    orderStatus.setSettlementAccountCurrency("");
                }
                orderStatus.setFundCode(respOrder.getAssetCode());


                ordersList.add(orderStatus);
            }
        } catch (Exception e) {
            long totalTimeTaken = new Date().getTime() - startTime.getTime();
            log.info("OLY_T24_PT Api Exception getActiveOrders from Olympus correlationId : {} , Rel Id : {} , Time Taken (ms) : {} " , correlationId, relationshipId, totalTimeTaken);
            log.error("OlyOrderStatusEnquiryClient:getOrders:Exception: {}", logUtil.printLog(
                    Constants.INTERFACE_NAME_OLY, e.getMessage()), e);
            throw new ApiException(new ApiErrorResponseBody(OrderApiErrorCodes.ORDER_STATUS_SVC_ERROR.getDescription(),
                    OrderApiErrorCodes.ORDER_STATUS_SVC_ERROR.getCode()));


        }

        try {
            List<com.scb.pbwm.omf.order.repository.Order> localOrders = orderRepository.findActiveOrderWithQueueStatus(this.getEncryptedValue(relationshipId));

            List<String> olyOrderNoList = ordersList.stream()
                    .filter(Objects::nonNull)
                    .filter(o -> !StringUtils.isEmpty(o.getOmfOrderNo()))
                    .map(o -> o.getOmfOrderNo().toUpperCase())
                    .collect(Collectors.toList());

            List<com.scb.pbwm.omf.order.repository.Order> filteredLocalOrders = localOrders.stream()
                    .filter(Objects::nonNull)
                    .filter(o -> !olyOrderNoList.contains(StringUtils.upperCase(o.getOmfOrderNo())))
                    .collect(Collectors.toList());

            filteredLocalOrders.stream().forEach(o -> {
                Orders orderStatus = new Orders();
                orderStatus.setActiveOrders(activeOrders);
                OrderResponse orderResp = o.getOrderDetails();

                TransactionTypeEnum displayTransactionType = TransactionTypeEnum.fromValue(o.getOrderType());

                //Replaced the transaction type for display purpose
                switch (displayTransactionType){
                    case LUMPSUM:
                        displayTransactionType= TransactionTypeEnum.SUB;
                        orderStatus.setOrderAmount(orderResp.getOrderAmount());
                        break;
                    case REDEMPTION:
                        displayTransactionType = TransactionTypeEnum.RED;
                        orderStatus.setOrderUnit(orderResp.getOrderAmount());
                        break;
                    case SWITCH:
                        displayTransactionType = TransactionTypeEnum.SWHO;
                        orderStatus.setOrderUnit(orderResp.getOrderAmount());
                        break;
                    case CANCEL:
                        displayTransactionType = TransactionTypeEnum.CON;
                        orderStatus.setOrderUnit(orderResp.getOrderAmount());
                        break;
                }
                orderStatus.setTransactionType(displayTransactionType);

                orderStatus.setInvestmentAccountNo(this.getDecryptedValue(orderResp.getInvestmentAccountNo()));
                orderStatus.setSettlementAccountNo(this.getDecryptedValue(orderResp.getSettlementAccountNo()));
                orderStatus.setSettlementAccountCurrency(orderResp.getSettlementAccountCurrency());
                orderStatus.setOmfOrderNo(o.getOmfOrderNo());
                orderStatus.setDividendInstruction(orderResp.getDividendInstruction());

                orderStatus.setOrderCcy(orderResp.getOrderCurrency());
                orderStatus.setOrderDate(orderResp.getOrderDate());

                orderStatus.setFundCode(orderResp.getAssestCode());

                //local recorded order default is online instead of branch
                String value = ChannelEnum.MBNK.getValue().equals(orderResp.getOrderChannel())?OrderChannelEnum.MOBILE.getValue():OrderChannelEnum.INTERNET_BANKING.getValue();
                orderStatus.setOrderChanelCode(value);

                String orderStatusDisplay = OrderQueueStatusEnum.FAILED.getValue().equals(o.getOrderStatus())?
                        OrderStatusEnum.REJECTED.getStatusDesc() : OrderStatusEnum.CAPTURED.getStatusDesc();
                orderStatus.setOrderStatus(orderStatusDisplay);
                ordersList.add(orderStatus);
            });

            Collections.sort(ordersList,(a, b) -> ordersComparator(a,b));

        }
        catch (Exception e) {
            log.error("OlyOrderStatusEnquiryClient:getOrders:Exception: {}", logUtil.printLog(
                    Constants.INTERFACE_NAME_DATABSE, e.getMessage()), e);
            throw new ApiException(new ApiErrorResponseBody(OrderApiErrorCodes.ORDER_STATUS_SVC_ERROR.getDescription(),
                    OrderApiErrorCodes.ORDER_STATUS_SVC_ERROR.getCode()));

        }


        return ordersList;
    }

    private int ordersComparator(Orders a, Orders b){
        //Null Last
        if(b.getOrderDate()==null){
            return a.getOrderDate()==null?0:-1;
        }
        if(a.getOrderDate()==null){
            return 1;
        }
        Date aOrderDate = parseOrderDate(a.getOrderDate());
        Date bOrderDate = parseOrderDate(b.getOrderDate());
        return bOrderDate.compareTo(aOrderDate);
    }

    private String getDecryptedValue(String cipheredText) {
        if (StringUtils.isBlank(cipheredText))
            return cipheredText;
        else
            return encryptionRestClient.getDecryptedValue(cipheredText);
    }

    private String getEncryptedValue(String text) {
        if (StringUtils.isBlank(text))
            return text;
        else
            return encryptionRestClient.getEncryptedValue(text);
    }


    String getCoolOffIndicator(LocalDate now, Order order) {
        if (StringUtils.isBlank(order.getCoolCancelPeriod())) {
            return N;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(YYYYMMDD);
        LocalDate coolOffEndDate = LocalDate.parse(order.getOrderDate(), formatter);
        int ccStrLength = order.getCoolCancelPeriod().length();
        char periodIndicator = order.getCoolCancelPeriod().charAt(ccStrLength - 1);
        int units = Integer.parseInt(order.getCoolCancelPeriod().substring(0, ccStrLength - 1));
        if (periodIndicator == DAY) {
            coolOffEndDate = coolOffEndDate.plusDays(units);
        } else if (periodIndicator == WEEK) {
            coolOffEndDate = coolOffEndDate.plusWeeks(units);
        } else if (periodIndicator == MONTH) {
            coolOffEndDate = coolOffEndDate.plusMonths(units);
        } else if (periodIndicator == YEAR) {
            coolOffEndDate = coolOffEndDate.plusYears(units);
        } else {
            log.error("Wrong periodIndicator for coolCancelPeriod " + order.getCoolCancelPeriod());
            throw new IllegalArgumentException("Wrong periodIndicator for coolCancelPeriod " + order.getCoolCancelPeriod());
        }
        return now.isBefore(coolOffEndDate) || now.isEqual(coolOffEndDate) ? Y : N;
    }

    private Date parseOrderDate(String orderDate) {
        Date date;
        if (com.scb.pbwm.omf.order.util.DateUtil.YYYY_MM_DD.length() == orderDate.length()) {
            date = com.scb.pbwm.omf.order.util.DateUtil.parseDate(orderDate, com.scb.pbwm.omf.order.util.DateUtil.YYYY_MM_DD);
        } else {
            date = com.scb.pbwm.omf.order.util.DateUtil.parseDate(orderDate, com.scb.pbwm.omf.order.util.DateUtil.DD_MM_YYYY_T_HH_MM_SS_SSS_UTC);
        }
        return date;
    }
}
