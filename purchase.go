package usecase

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
	"github.com/tabbed/pqtype"
	"zebrax.id/emi/integration/core/proto"
	utils "zebrax.id/emi/integration/core/utils"
	"zebrax.id/emi/integration/erp/adapter/repository/query"
	odooConnectorModel "zebrax.id/emi/integration/erp/connector/odoo/model"
)

func OrderData(orderConfirm odooConnectorModel.PreOrderResponse) (result proto.PurchaseDetailResponse) {

	responseDetail := orderConfirm.ResponseDetail
	preOrderResponseDetail := orderConfirm.ResponseDetail.OrderConfirmationResponses

	log.Info(fmt.Printf("[PreOrder Confirmation] Response: %#v\n", orderConfirm))

	result.Status = utils.ConstructStatus(nil, orderConfirm.Message, orderConfirm.Code == "0")

	// Collect Purchase
	totalItemPurchase, _ := utils.StringToInt32(strings.ReplaceAll(responseDetail.Purchase.Total, ".", ""))
	itemsPurchase, _ := extractAttributes(responseDetail.Purchase.Items)

	// Collect Administration
	totalAdmsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(responseDetail.Administrations.Total, ".", ""))
	addAdminTax(&preOrderResponseDetail)
	admsPurchase, _ := extractAttributes(responseDetail.Administrations.Items)

	// Collect Reductions
	totalReductionsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(responseDetail.Reductions.Total, ".", ""))
	reductionsVoucherPurchase, reductionsDiscountPurchase := extractAttributes(responseDetail.Reductions.Items)

	grandTotal, _ := utils.StringToInt32(responseDetail.GrandTotal)

	remainingAmount, _ := strconv.Atoi(responseDetail.RemainingAmount)
	bookingFeeAmount, _ := strconv.Atoi(responseDetail.BookingFeeAmount)

	result.OrderData = &proto.Order{
		Purchase: &proto.OrderComponent{
			Items: itemsPurchase,
			Total: totalItemPurchase,
		},
		Administration: &proto.OrderComponent{
			Items: admsPurchase,
			Total: totalAdmsPurchase,
		},
		Reduction: &proto.OrderComponent{
			Vouchers:  reductionsVoucherPurchase,
			Discounts: reductionsDiscountPurchase,
			Total:     totalReductionsPurchase * -1, //temporarily using this method
		},
		Total:            grandTotal,
		SalesOrderID:     responseDetail.SoID,
		SalesOrderNumber: responseDetail.SoNumber,
		RemainingAmount:  int32(remainingAmount),
	}
	result.Product = &proto.ProductVariant{
		BookingFeeAmount: int32(bookingFeeAmount),
	}

	return result
}

func (r *useCase) DealerList(ctx context.Context, in *proto.DealerListParams) (result *proto.PurchaseListResponse, err error) {
	log.Info("Start Request Dealer List")
	defer log.Debug("Dealer List Response: ", result, err)

	var (
		protoDealers = []*proto.DealerData{}
		respCode     = 200
	)

	dealers, err := r.oRepo.GetDealerAndDefault(in.OdooID, in.Longitude, in.Latitude)
	if err != nil {
		respCode = 500
		return result, err
	}

	for _, each := range dealers {
		zipCode, _ := utils.StringToInt32(each.ZipCode)
		protoDealers = append(protoDealers, &proto.DealerData{
			Id:             int32(each.Id),
			Location:       each.Name,
			Address1:       each.Address1,
			Address2:       each.Address2,
			City:           each.City,
			State:          each.Province,
			Country:        each.Country,
			Latitude:       each.Latitude,
			Longitude:      each.Longitude,
			OperatingHours: each.OperatingHours,
			ZipCode:        zipCode,
			Code:           each.Code,
			Distance:       each.Distance,
			DistanceLable:  each.DistanceUnit,
			Default:        each.Default,
		})
	}

	result = &proto.PurchaseListResponse{
		DealerData: protoDealers,
		EvData:     []*proto.EvData{},
	}

	result.Status = utils.ConstructStatus(nil, "Odoo Request Error", respCode != 200)
	return result, nil
}

func (r *useCase) ProductPrice(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("Start Product Price")
	defer log.Debug("Product Price Response: ", result, err)

	result = new(proto.PurchaseDetailResponse)
	dealerID, _ := strconv.Atoi(in.DealerID)
	productTemplates, err := r.oRepo.GetProductTemplatePrice(int32(dealerID), in.ProductCode)
	if err != nil {
		log.Error("[GetProductTemplatePrice] Error:", err)
	}

	price := 0.0
	code := ""
	name := ""
	bookingFeeAmount := 0.0
	templateAttributes := []*proto.Attribute{}
	for _, item := range productTemplates {
		templateAttributes = []*proto.Attribute{}
		utils.CopyObject(item.Attributes, &templateAttributes)
		price, _ = utils.StringToFloat64(item.MinUnitPrice)
		code = item.ProductTemplateCode
		name = item.ProductTemplateName
		bookingFeeAmount, _ = utils.StringToFloat64(item.BookingFeeAmount)
	}

	result = &proto.PurchaseDetailResponse{
		Product: &proto.ProductVariant{
			Code:             code,
			Name:             name,
			MinPrice:         int32(price),
			Attributes:       templateAttributes,
			BookingFeeAmount: int32(bookingFeeAmount),
		},
	}

	return result, nil
}

func (r *useCase) OrderConfirmation(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("[Order Confirmation] Start")
	defer log.Info("[Order Confirmation] End")

	var (
		orderConfirmation = odooConnectorModel.OrderConfirmationResponses{}
	)

	result = new(proto.PurchaseDetailResponse)

	purchaseParams := odooConnectorModel.PurchaseParams{}
	utils.CopyObject(in, &purchaseParams)

	log.Info(fmt.Printf("[Order Confirmation] Start params: %#v\n", purchaseParams))
	if in.SalesOrderID != "" {
		salesOrderID, _ := utils.StringToInt32(in.SalesOrderID)
		if in.VoucherID != "" {
			voucherID, _ := utils.StringToInt32(in.VoucherID)
			orderConfirmation, err = r.oRepo.SetVoucherRedeem(salesOrderID, voucherID)
			if err != nil {
				log.Error("[Error SetVoucherRedeem Order Confirmation]-", err)
				return result, err
			}
		}

		if in.PaymentTypeID != "" {
			orderConfirmation, err = r.oRepo.SetPaymentMethod(salesOrderID, in.PaymentTypeID)
			if err != nil {
				log.Error("[Error SetPaymentMethod Order Confirmation]-", err)
				return result, err
			}
		} else {
			r.oRepo.ResetPaymentMethod(salesOrderID)
		}

	}

	orderConfirmation, err = r.oRepo.SetOrderConfirmation(purchaseParams)
	if err != nil {
		log.Error("[Error SetOrderConfirmation Order Confirmation]-", err)
		return result, err
	}

	log.Info(fmt.Printf("[Order Confirmation] Response: %#v\n", orderConfirmation))

	result.Status = utils.ConstructStatus(nil, orderConfirmation.Message, orderConfirmation.Code == "0")

	// Collect Purchase
	totalItemPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Purchase.Total, ".", ""))
	itemsPurchase, _ := extractAttributes(orderConfirmation.Purchase.Items)

	// Collect Administration
	totalAdmsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Administrations.Total, ".", ""))
	addAdminTax(&orderConfirmation)
	admsPurchase, _ := extractAttributes(orderConfirmation.Administrations.Items)

	// Collect Reductions
	totalReductionsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Reductions.Total, ".", ""))
	reductionsVoucherPurchase, reductionsDiscountPurchase := extractAttributes(orderConfirmation.Reductions.Items)

	grandTotal, _ := utils.StringToInt32(orderConfirmation.GrandTotal)

	result.OrderData = &proto.Order{
		Purchase: &proto.OrderComponent{
			Items: itemsPurchase,
			Total: totalItemPurchase,
		},
		Administration: &proto.OrderComponent{
			Items: admsPurchase,
			Total: totalAdmsPurchase,
		},
		Reduction: &proto.OrderComponent{
			Vouchers:  reductionsVoucherPurchase,
			Discounts: reductionsDiscountPurchase,
			Total:     totalReductionsPurchase * -1, //temporarily using this method
		},
		Total:            grandTotal,
		SalesOrderID:     orderConfirmation.SoID,
		SalesOrderNumber: orderConfirmation.SoNumber,
	}

	return result, nil
}

func (r *useCase) PurchaseStock(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("Start Purchase Stock")
	defer log.Debug("Purchase Stock Response: ", result, err)

	result = new(proto.PurchaseDetailResponse)

	templateAttributes := odooConnectorModel.PurchaseParams{}
	utils.CopyObject(in, &templateAttributes)

	purchaseStock, _ := r.oRepo.GetProductStock(templateAttributes)

	result.Product = &proto.ProductVariant{
		Attributes: []*proto.Attribute{
			{
				ProductCode: purchaseStock.ProductCode,
				Stock:       purchaseStock.Qty,
			},
		},
	}

	return result, nil
}

func (r *useCase) Payment(ctx context.Context, in *proto.PaymentParams) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("Start Payment")
	defer log.Debug("Payment Response: ", result, err)

	result = new(proto.PurchaseDetailResponse)

	paymentParams := odooConnectorModel.PaymentParams{}
	utils.CopyObject(in, &paymentParams)
	orderConfirmation, err := r.oRepo.SetPayment(paymentParams)
	if err != nil {
		log.Info(orderConfirmation.Error)
		return result, nil
	}

	if orderConfirmation.Code == "1" {
		log.Info(orderConfirmation.Message)
	} else {
		log.Info("[Payment] Insert into Purchase Log")
		orderConfirmationMarshal, _ := json.Marshal(orderConfirmation)
		orderConfirmationRaw := json.RawMessage(
			string(orderConfirmationMarshal),
		)

		err = r.repo.InsertPurchaseLog(ctx, &query.CreatePurchaseLogParams{
			InvoiceID: orderConfirmation.InvoiceNumber,
			Payload:   pqtype.NullRawMessage{RawMessage: orderConfirmationRaw, Valid: true},
		})
		if err != nil {
			log.Info("[Payment] Insert into Purchase Log Error : ", err.Error())
		}

	}

	// Collect Purchase
	totalItemPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Purchase.Total, ".", ""))
	itemsPurchase, _ := extractAttributes(orderConfirmation.Purchase.Items)

	// Collect Administration
	totalAdmsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Administrations.Total, ".", ""))
	addAdminTax(&orderConfirmation)
	admsPurchase, _ := extractAttributes(orderConfirmation.Administrations.Items)

	// Collect Reductions
	totalReductionsPurchase, _ := utils.StringToInt32(strings.ReplaceAll(orderConfirmation.Reductions.Total, ".", ""))
	reductionsVoucherPurchase, reductionsDiscountPurchase := extractAttributes(orderConfirmation.Reductions.Items)

	grandTotal, _ := utils.StringToInt32(orderConfirmation.GrandTotal)

	result = &proto.PurchaseDetailResponse{
		OrderData: &proto.Order{
			Purchase: &proto.OrderComponent{
				Items: itemsPurchase,
				Total: totalItemPurchase,
			},
			Administration: &proto.OrderComponent{
				Items: admsPurchase,
				Total: totalAdmsPurchase,
			},
			Reduction: &proto.OrderComponent{
				Vouchers:  reductionsVoucherPurchase,
				Discounts: reductionsDiscountPurchase,
				Total:     totalReductionsPurchase * -1, //temporarily using this method
			},
			Total:            grandTotal,
			SalesOrderID:     orderConfirmation.SoID,
			SalesOrderNumber: orderConfirmation.SoNumber,
			InvoiceID:        orderConfirmation.InvoiceID,
			InvoiceNumber:    orderConfirmation.InvoiceNumber,
			ExpiredTime:      orderConfirmation.ExpiredTime,
		},
	}

	return result, nil
}

func (r *useCase) PaymentNotification(ctx context.Context, in *proto.PaymentParams) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("Start PaymentNotification")
	defer log.Debug("PaymentNotification Response: ", result, err)

	var (
		code bool = true
	)

	paymentParams := odooConnectorModel.PaymentParams{}
	utils.CopyObject(in, &paymentParams)
	paymentNotification, err := r.oRepo.SetPaymentNotification(paymentParams)
	if err != nil {
		code = false
		log.Info(err.Error())
	}

	if paymentNotification.Code == "1" {
		log.Info(paymentNotification.Message)
	}

	err = r.repo.UpdatePurchaseLogState(ctx, &query.UpdatePurchaseLogStateParams{
		InvoiceID:   paymentParams.InvoiceNumber,
		State:       sql.NullString{String: paymentParams.Status, Valid: true},
		UpdatedTime: sql.NullTime{Time: utils.TimeToRoundNanoSecond(time.Now()), Valid: true},
	})
	if err != nil {
		log.Info("[PaymentNotification] Update Purchase Log State Error : ", err.Error())
	}

	result = new(proto.PurchaseDetailResponse)

	result = &proto.PurchaseDetailResponse{
		Success: code,
		Message: paymentNotification.Message,
	}

	return result, nil
}

func (r *useCase) VoucherList(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseListResponse, err error) {
	log.Debug("Start VoucherList")
	defer log.Debug("VoucherList Response: ", result, err)

	salesOrderID, _ := strconv.Atoi(in.SalesOrderID)
	voucherList, err := r.oRepo.GetVoucherList(int32(salesOrderID))

	result = new(proto.PurchaseListResponse)
	result.Status = utils.ConstructStatus(nil, "Odoo error", err != nil)
	result.Vouchers = []*proto.VoucherData{}
	if !result.Status.Success || len(voucherList) == 0 {
		return
	}

	for _, voucher := range voucherList {
		var tncs []*proto.DescriptionData

		for _, tnc := range voucher.Tnc {
			tncs = append(tncs, &proto.DescriptionData{
				Description: tnc.Description,
			})
		}
		result.Vouchers = append(result.Vouchers, &proto.VoucherData{
			ID:          utils.InterfaceToString(voucher.ID),
			Name:        voucher.Name,
			Quota:       int32(voucher.Quota),
			DealerID:    utils.InterfaceToString(voucher.DealerID),
			DealerCode:  voucher.DealerCode,
			DealerName:  voucher.DealerName,
			VoucherCode: voucher.VoucherCode,
			ValidUntil:  voucher.ValidUntil,
			Label:       voucher.Label,
			Title:       voucher.Title,
			Minimum:     int32(voucher.Minimum),
			Tnc:         tncs,
			Available:   voucher.Available,
		})
	}
	return
}

func (r *useCase) BOStatusOrder(ctx echo.Context) (result *proto.StatusNotificationResponse, err error) {
	log.Info("[Webhook] OrderStatus Start")

	json_map := new(proto.StatusNotificationInput)

	if err = json.NewDecoder(ctx.Request().Body).Decode(&json_map); err != nil {
		log.Error("[Webhook] OrderStatus Parsing Payload Error: ", err)
		return nil, err
	}
	log.Debug("[Webhook] OrderStatus Param: ", json_map)

	err = r.repo.UpdatePurchaseLogState(ctx.Request().Context(), &query.UpdatePurchaseLogStateParams{
		InvoiceID:   json_map.InvoiceNumber,
		State:       sql.NullString{String: json_map.Status, Valid: true},
		UpdatedTime: sql.NullTime{Time: time.Now(), Valid: true},
	})
	if err != nil {
		log.Info("[BOStatusOrder] Update Purchase Log State Error : ", err.Error())
	}

	return r.vendureClient.SendOrderStatus(json_map)
}

func (r *useCase) LicenceStatus(ctx echo.Context) (result *proto.LicensePlateStatusNotificationResponse, err error) {
	log.Info("[Webhook] LicenceStatus Start")

	json_map := new(proto.LicensePlateStatusNotificationInput)

	if err = json.NewDecoder(ctx.Request().Body).Decode(&json_map); err != nil {
		log.Error("[Webhook] LicenceStatus Parsing Payload Error: ", err)
		return nil, err
	}
	log.Debug("[Webhook] LicenceStatus Param: ", json_map)

	return r.vendureClient.SendPlateStatus(json_map)
}

func extractAttributes(attrs []odooConnectorModel.OrderConfirmationAttributes) (orderItem []*proto.OrderItem, discountOrderItem []*proto.OrderItem) {
	for _, item := range attrs {
		attributeItems := []*proto.Attribute{}
		utils.CopyObject(item.Attributes, &attributeItems)

		value, _ := utils.StringToInt32(item.OdooValue)
		label := item.Label
		if value < 0 {
			value *= -1
			label = strings.ReplaceAll(item.Label, "-", "")
		}
		itemPurchase := &proto.OrderItem{
			Name:       item.OdooName,
			Value:      value,
			Label:      label,
			Attributes: attributeItems,
		}

		if item.ReductionType == "discount" {
			discountOrderItem = append(orderItem, itemPurchase)
		} else {
			orderItem = append(orderItem, itemPurchase)
		}
	}

	return orderItem, discountOrderItem
}

func addAdminTax(result *odooConnectorModel.OrderConfirmationResponses) {
	if result.Tax != "" {
		tax, _ := utils.StringToInt(result.Tax)
		total, _ := utils.StringToInt(result.Administrations.Total)
		result.Administrations.Items = append(result.Administrations.Items, odooConnectorModel.OrderConfirmationAttributes{
			OdooName:  "Tax",
			OdooValue: result.Tax,
			Label:     result.Tax,
		})
		total += tax
		result.Administrations.Total = fmt.Sprintf("%d", total)
	}
}

func (r *useCase) SetPreOrderPaymentStatus(ctx context.Context, in *proto.PaymentParams) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("Start PreOrderSetPaymentStatus")
	defer log.Debug("PreOrderSetPaymentStatus Response: ", result, err)

	var (
		code bool = true
	)

	paymentParams := odooConnectorModel.PaymentParams{}
	utils.CopyObject(in, &paymentParams)
	paymentNotification, err := r.oRepo.SetPreOrderPaymentStatus(paymentParams)
	if err != nil {
		code = false
		log.Info(err.Error())
	}

	if paymentNotification.Code == "1" {
		log.Info(paymentNotification.Message)
	}

	result = new(proto.PurchaseDetailResponse)

	result = &proto.PurchaseDetailResponse{
		Success: code,
		Message: paymentNotification.Message,
	}

	return result, nil
}

func (r *useCase) PreOrderConfirmation(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("[PreOrder Confirmation] Start")
	defer log.Info("[PreOrder Confirmation] End")

	var (
		orderConfirmation = odooConnectorModel.PreOrderResponse{}
	)

	result = new(proto.PurchaseDetailResponse)

	purchaseParams := odooConnectorModel.PurchaseParams{}
	utils.CopyObject(in, &purchaseParams)

	log.Info(fmt.Printf("[PreOrder Confirmation] Start params: %#v\n", purchaseParams))
	if in.SalesOrderID != "" {
		salesOrderID, _ := utils.StringToInt32(in.SalesOrderID)

		if in.PaymentTypeID != "" {
			orderConfirmation, err = r.oRepo.SetPreOrderPaymentMethod(salesOrderID, in.PaymentTypeID)
			if err != nil {
				log.Error("[Error SetPaymentMethod PreOrder Confirmation]-", err)
				return result, err
			}
		} else {
			r.oRepo.ResetPreOrderPaymentMethod(salesOrderID)
		}

	}

	orderConfirmation, err = r.oRepo.SetPreOrderConfirmation(purchaseParams)
	if err != nil {
		log.Error("[Error SetPreOrderConfirmation PreOrder Confirmation]-", err)
		return result, err
	}

	orderResult := OrderData(orderConfirmation)

	return &orderResult, nil
}

func (r *useCase) PreOrderPaymentConfirm(ctx context.Context, in *proto.PurchaseParam) (result *proto.PurchaseDetailResponse, err error) {
	log.Info("[PreOrder Confirmation] Start")
	defer log.Info("[PreOrder Confirmation] End")

	var (
		orderConfirmation = odooConnectorModel.PreOrderResponse{}
	)

	result = new(proto.PurchaseDetailResponse)

	purchaseParams := odooConnectorModel.PurchaseParams{}
	utils.CopyObject(in, &purchaseParams)

	salesOrderID, _ := utils.StringToInt32(in.SalesOrderID)

	orderConfirmation, err = r.oRepo.PreOrderPaymentConfirm(salesOrderID)
	if err != nil {
		log.Error("[Error SetPreOrderConfirmation PreOrder Confirmation]-", err)
		return result, err
	}

	orderResult := OrderData(orderConfirmation)

	return &orderResult, nil
}
